from __future__ import annotations

import fnmatch
import hashlib
import logging
import os.path
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable
from xml.etree import ElementTree

import click
import git
from P4 import P4

LOG = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

MAX_CPU_COUNT = os.cpu_count() or 8


def compute_digest(path: str, file_type: str) -> str:
    """
    Compute the MD5 digest of a file, knowing it's expected type (e.g. from perforce).
    If the file type has changed, the digests will just mismatch and so perforce will handle
    actually comparing the difference, which is nbd.
    """
    # perforce stores/hashes all text files with LF line endings, convert any CRLF -> LF
    # also remove BOM from utf files if present, since p4 seems to also ignore them when hashing
    h = hashlib.md5()
    with open(path, "rb") as f:
        if "text" in file_type:
            for line in f:
                line = line.replace(b"\r\n", b"\n")
                h.update(line)

        elif "utf8" in file_type:
            for line in f:
                # read in utf-8-sig to strip BOMs
                line = line.decode("utf-8-sig").encode("utf-8").replace(b"\r\n", b"\n")
                h.update(line)

        elif "utf16" in file_type:
            # read in utf-16 to strip BOMs, p4 seems to always hash in utf-8...
            data = f.read().decode("utf-16").encode("utf-8").replace(b"\r\n", b"\n")
            h.update(data)

        else:
            # binary
            # 64KB seems to be a good chuk size for both small and large files
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)

    # p4 digests are all uppercase
    return h.hexdigest().upper()


class GitFile(object):
    def __init__(self, root: str, path: str, is_tracked=True):
        self.root = root
        self.relative_path = path
        self.full_path = os.path.join(self.root, self.relative_path)
        self.digest = None
        # is the file actually tracked in git? or some additional dependency synced from elsewhere
        self.is_tracked = is_tracked

    def __repr__(self):
        return f"<GitFile '{self.relative_path}'>"

    def __str__(self):
        return self.relative_path

    def compute_digest(self, file_type: str) -> str:
        if not self.digest:
            self.digest = compute_digest(self.full_path, file_type)
        return self.digest


class P4File(object):
    def __init__(self, fstat: dict, client_root: str):
        self.depot_path: str = fstat["depotFile"]
        self.client_path: str = fstat["clientFile"]
        self.head_action: str = fstat["headAction"]
        self.head_type: str = fstat["headType"]
        self.head_change: str = fstat["headChange"]
        self.file_size: str = fstat["fileSize"]
        self.digest: str = fstat["digest"]
        # important that this is normalized, since it's used for comparison
        self.relative_path = os.path.relpath(self.client_path, client_root).replace("\\", "/")

    def __repr__(self):
        return f"<P4File '{self.relative_path}'>"

    def __str__(self):
        return self.relative_path


class P4GitFileDiff(object):
    """
    Represents a diff between a source Git repo and destination P4 stream.

    GitFiles don't actually have to be tracked (there can be extras) but usually
    come from git ls-tree. Content-only differences are checked first by file size
    (for binary files only, since ascii line endings can affect this),
    then by computing a digest for the source files and comparing with p4's digest.

    The results are in the lists `src_only`, `dst_only`, `case_mismatch` and `changed`.
    """

    def __init__(self, src_files: list[GitFile], dst_files: list[P4File]):
        self.src_files = src_files
        self.dst_files = dst_files

        self.src_only: list[GitFile] = []
        self.dst_only: list[P4File] = []
        self.case_mismatch: list[tuple[GitFile, P4File]] = []
        self.changed: list[tuple[GitFile, P4File]] = []

        self.compare()

    def compare(self):
        start = time.perf_counter()
        LOG.info(f"Comparing {len(self.src_files)} source files against {len(self.dst_files)} destination files...")
        src_map = {src.relative_path.lower(): src for src in self.src_files}
        dst_map = {dst.relative_path.lower(): dst for dst in self.dst_files}
        src_paths = set(src_map.keys())
        dst_paths = set(dst_map.keys())

        # find exclusive paths
        self.src_only = [src_map[p] for p in src_paths - dst_paths]
        self.dst_only = [dst_map[p] for p in dst_paths - src_paths]

        # compare matching files
        same_paths = [(src_map[p], dst_map[p]) for p in src_paths & dst_paths]
        LOG.info(f"Comparing {len(same_paths)} files with matching paths...")

        with ThreadPoolExecutor(max_workers=MAX_CPU_COUNT) as executor:
            for i, result in enumerate(executor.map(self.compare_matching, same_paths)):
                case_differs, content_differs, src, dst = result
                if case_differs:
                    self.case_mismatch.append((src, dst))
                elif content_differs:
                    self.changed.append((src, dst))
                if i > 0 and i % 50000 == 0:
                    LOG.info(f"[{i}/{len(same_paths)}]")
        end = time.perf_counter()
        LOG.info(f"Finished comparison ({end - start:.1f}s)")

    @staticmethod
    def compare_matching(src_and_dst: tuple[GitFile, P4File]) -> tuple[bool, bool, GitFile, P4File]:
        # check for differences in case
        src, dst = src_and_dst
        if src.relative_path != dst.relative_path:
            return True, False, src, dst

        # quick check size (binary files only, since line endings can affect this)
        if dst.head_type.startswith("binary"):
            size = os.stat(src.full_path).st_size
            if str(size) != dst.file_size:
                return False, True, src, dst

        # compute and check digest
        src.compute_digest(dst.head_type)
        if src.digest != dst.digest:
            return False, True, src, dst

        # same contents
        return False, False, src, dst

    def has_difference(self) -> bool:
        return bool(self.changed or self.src_only or self.dst_only)

    def log_diff(self):
        LOG.info(f"Source only: {len(self.src_only)}")
        LOG.info(f"Destination only: {len(self.dst_only)}")
        LOG.info(f"Case mismatch: {len(self.case_mismatch)}")
        LOG.info(f"Content Changed: {len(self.changed)}")


def load_toml_config(file_path: str) -> dict:
    with open(file_path, "rb") as fp:
        return tomllib.load(fp)


class P4HarmonizeGit(object):
    # the default set of source files to ignore, as fnmatch patterns against relative paths
    default_src_ignore = []

    # the default set of destination files to ignore (to not delete them), as fnmatch patterns against relative paths
    default_dst_ignore = []

    def __init__(self, config_file: str, dry_run=False):
        self.config = load_toml_config(config_file)
        self.src_ignore = self.config["source"].get("ignore", self.default_src_ignore)
        self.dst_ignore = self.config["destination"].get("ignore", self.default_dst_ignore)
        self._p4 = None
        self.p4_info = None
        self.dry_run = dry_run

    @property
    def p4(self) -> P4:
        self.ensure_p4_connection()
        return self._p4

    def ensure_p4_connection(self):
        if self._p4 is None:
            self._p4 = self.create_p4_connection()
            # also gather p4 info for case-sensitive checks
            self.p4_info = self._p4.run_info()[0]

    def create_p4_connection(self) -> P4:
        p4 = P4()
        dest_config = self.config["destination"]
        p4.port = dest_config["p4port"]
        p4.user = dest_config["p4user"]
        p4.client = dest_config["p4client"]

        p4.connect()
        p4.exception_level = p4.RAISE_ERRORS

        if not p4.connected():
            LOG.error("Failed to create P4 connection")
            sys.exit(1)
        return p4

    def is_p4_case_sensitive(self):
        self.ensure_p4_connection()
        return self.p4_info["clientCase"] != "insensitive"

    def p4_run(self, args, always_run=False):
        LOG.info(f"p4 {' '.join(args)}")
        if not self.dry_run or always_run:
            result = self.p4.run(*args)
        else:
            result = None
        for warning in self.p4.warnings:
            LOG.warning(warning)
        return result

    def p4_cmd(self, args, always_run=False):
        args = ["p4", "-p", self.p4.port, "-c", self.p4.client, "-u", self.p4.user] + args
        LOG.info(f"{subprocess.list2cmdline(args)}")
        if not self.dry_run or always_run:
            return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return None

    def p4_cmd_batch(self, args, files: Iterable[str], prefix: str, always_run=False):
        """
        Run p4 commandline, passing a list of files in by writing it to a temp file and using '-x <tmpfile>'.
        Used to bypass command line length limits for large amounts of files.
        """
        if not self.dry_run or always_run:
            with tempfile.NamedTemporaryFile(mode="w+", delete=True, delete_on_close=False, prefix=prefix) as tmp:
                tmp_path = tmp.name.replace("\\", "/")
                tmp.writelines([f"{f}\n" for f in files])
                tmp.flush()
                tmp.close()
                self.p4_cmd(["-x", tmp_path, *args])
        else:
            LOG.info(f"p4 -x ... {subprocess.list2cmdline(args)}")

    def list_git_files(self, git_dir: str) -> list[GitFile]:
        g = git.cmd.Git(git_dir)
        paths = g.ls_tree(["-r", "--name-only", "HEAD"]).split("\n")
        result = [GitFile(git_dir, p) for p in paths]
        return result

    def list_p4_files(self, depot_path: str) -> list[P4File]:
        args = ["fstat", "-T depotFile,clientFile,headAction,headChange,headType,fileSize,digest", "-Ol", depot_path]
        fstats = self.p4_run(args, True)
        if fstats and "clientFile" not in fstats[0]:
            LOG.error("Failed to get 'clientFile' from fstat, make sure destination client exists")
            sys.exit(1)

        client_root = self.config["destination"]["root"]
        return [P4File(fstat, client_root) for fstat in fstats]

    def ensure_dst_p4_client_doesnt_exist(self):
        client_name = self.config["destination"]["p4client"]
        existing_client = self.p4_run(["clients", "-e", client_name])
        if existing_client:
            LOG.error(f"Destination p4 client already exists: {client_name}, run clean first or delete it manually")
            sys.exit(1)

    def create_dest_p4_client(self):
        client_name = self.config["destination"]["p4client"]
        LOG.info(f"Creating p4 client {self.config['destination']['p4client']}")
        client = self.p4_run(["client", "-o", client_name])[0]
        client._root = self.config["destination"]["root"]
        client._stream = self.config["destination"]["stream"]
        client["Options"] = "noallwrite noclobber nocompress unlocked modtime rmdir noaltsync"
        client["SubmitOptions"] = "leaveunchanged"
        LOG.info("p4 client -i ...")
        if not self.dry_run:
            self.p4.save_client(client)

    def create_dest_p4_changelist(self, description: str) -> str:
        # TODO: create a changelist with the description and use it when opening files
        return "123"

    def delete_p4_client(self, p4client: str):
        clients = self.p4_run(["clients", "-e", p4client])
        if not clients:
            LOG.info(f"Client not found: {p4client}")
            return
        else:
            LOG.info(f"Deleting client: {p4client}")

        if not self.dry_run:
            self.p4_run(["client", "-df", p4client])

    def should_ignore_src(self, src: GitFile):
        for pattern in self.src_ignore:
            if fnmatch.fnmatch(src.relative_path, pattern):
                return True
        return False

    def should_ignore_dst(self, dst: P4File):
        for pattern in self.dst_ignore:
            if fnmatch.fnmatch(dst.relative_path, pattern):
                return True
        return False

    def get_src_files(self) -> list[GitFile]:
        src_files = self.list_git_files(self.config["source"]["root"])
        LOG.info(f"Found {len(src_files)} source files from git ls-tree")
        return src_files

    def get_dst_files(self) -> list[P4File]:
        p4_stream = self.config["destination"]["stream"]
        dst_files = self.list_p4_files(f"{p4_stream}/...")
        LOG.info(f"Found {len(dst_files)} tracked files in the destination: {p4_stream}")
        return dst_files

    def get_diff(self) -> P4GitFileDiff:
        LOG.info(f"Finding files in source ({self.config['source']['root']})...")
        src_files = self.get_src_files()
        src_files = [src for src in src_files if not self.should_ignore_src(src)]
        LOG.info(f"Found {len(src_files)} total source files (after ignores)")

        # list files from dest depot
        LOG.info(f"Finding files in destination ({self.config['destination']['stream']})...")
        dst_files = self.get_dst_files()
        dst_files = [dst for dst in dst_files if not self.should_ignore_dst(dst)]
        LOG.info(f"Found {len(dst_files)} total destination files (after ignores)")

        # calculate the actual difference (src only, dst only, changed, etc.)
        return P4GitFileDiff(src_files, dst_files)

    def run_validate(self):
        """
        Perform some initial checks to make sure we can run successfully.
        """
        self.ensure_dst_p4_client_doesnt_exist()

        dest_root = self.config["destination"]["root"]
        if os.path.isdir(dest_root) and os.listdir(dest_root):
            LOG.error(f"Destination root must be empty: {dest_root}")
            sys.exit(1)

    def pre_run(self):
        pass

    def post_run(self, diff: P4GitFileDiff):
        pass

    def run(self):
        start = time.perf_counter()
        self.run_validate()

        self.pre_run()

        # create destination workspace, must be done first so we can get clientFile
        # paths from p4 fstat (to match relative paths from src -> dst)
        self.create_dest_p4_client()

        diff = self.get_diff()
        diff.log_diff()

        if not diff.has_difference():
            LOG.info("All files in source and destination already match")
            self.post_run(diff)
            return

        # slam files to head revision without syncing anything
        p4_stream = self.config["destination"]["stream"]
        self.p4_run(["flush", f"{p4_stream}/..."])

        # copy files to dest
        files_to_add = [src for src in diff.src_only]
        files_to_edit = [src for src, dst in diff.changed]
        files_to_copy = files_to_add + files_to_edit

        LOG.info(f"Copying {len(files_to_copy)} files to destination workspace...")
        with ThreadPoolExecutor(max_workers=MAX_CPU_COUNT) as executor:
            list(executor.map(self.copy_file_to_dst, files_to_copy))

        # mark files for delete (`dst_only`, as well as `case_mismatch` on a case-insensitive server)
        files_to_delete = {dst.client_path for dst in diff.dst_only}

        if diff.case_mismatch:
            if self.is_p4_case_sensitive():
                # check out and move case mismatches (individually)
                for src, dst in diff.case_mismatch:
                    self.p4_run(["edit", dst.client_path])
                    self.p4_run(["move", dst.client_path, self.get_dst_path(src)])
            else:
                # mark case mismatches for delete, and require a re-run of this tool
                LOG.warning(
                    "Found files that differ in case-only, but the destination server is case insensitive. "
                    "Perforce can't fix case issues on a case insensitive server in one submit. "
                    "The mismatching files will be staged for delete. After submitting, re-run this tool "
                    "to re-add the deleted files with the correct case. "
                    "See https://portal.perforce.com/s/article/3448 for more details."
                )
                for src, dst in diff.case_mismatch:
                    LOG.debug(f"Case change: {dst.relative_path} -> {src.relative_path}")
                files_to_delete.update({dst.client_path for src, dst in diff.case_mismatch})

        if files_to_delete:
            self.p4_cmd_batch(["delete"], files_to_delete, "p4harmonize_git_delete_")

        # check out files
        if files_to_add:
            self.p4_cmd_batch(["add"], [self.get_dst_path(f) for f in files_to_add], "p4harmonize_git_add_")
        if files_to_edit:
            self.p4_cmd_batch(["edit"], [self.get_dst_path(f) for f in files_to_edit], "p4harmonize_git_edit_")

        # revert unchanged files
        self.p4_run(["revert", "-a"])

        self.post_run(diff)

        end = time.perf_counter()
        minutes, secs = divmod(end - start, 60)
        LOG.info(f"Finished ({minutes}m {secs:.1f}s)")
        LOG.info("Any changes are staged p4, review and submit when ready.")

    def get_dst_path(self, src: GitFile) -> str:
        return os.path.join(self.config["destination"]["root"], src.relative_path).replace("\\", "/")

    def copy_file_to_dst(self, src: GitFile):
        dst_path = self.get_dst_path(src)
        if not self.dry_run:
            os.makedirs(os.path.dirname(dst_path), exist_ok=True)
            shutil.copy2(src.full_path, dst_path, follow_symlinks=False)

    def clean(self):
        """
        Clean up destination workspace and files.
        """
        p4client = self.config["destination"]["p4client"]
        self.delete_p4_client(p4client)

        dest_root = self.config["destination"]["root"]
        if os.path.isdir(dest_root):
            LOG.info(f"Deleting destination root: {dest_root}")
            if not self.dry_run:
                shutil.rmtree(dest_root)
        else:
            LOG.info(f"Destination root not found: {dest_root}")


class P4HarmonizeGitUnreal(P4HarmonizeGit):
    git_dependencies_path = "Engine/Binaries/DotNET/GitDependencies/win-x64/GitDependencies.exe"
    ue_dependencies_path = ".uedependencies"

    # if not specified in the config, the default set of files
    # to ignore, specific to GitHub UnrealEngine
    default_src_ignore = [
        ".gitattributes",
        ".gitignore",
        ".tgitconfig",
        "LICENSE.md",
        "PULL_REQUEST_TEMPLATE.md",
        "README.md",
        "SECURITY.md",
        "Setup.bat",
        "Setup.command",
        "Setup.sh",
        "*.DS_Store/*",
    ]

    default_dst_ignore = [
        ".p4ignore.txt",
        "RunUAT.bat",
        "RunUAT.sh",
        "RunUBT.bat",
        "RunUBT.sh",
        "vs-chromium-project.txt",
    ]

    def pre_run(self):
        """
        Update Unreal Git dependencies.
        """
        super().pre_run()

        if not self.dry_run:
            self.update_ue_dependencies()

    def get_src_files(self):
        src_files = super().get_src_files()
        dep_files = self.get_ue_dependencies()
        LOG.info(f"Found {len(dep_files)} source files from {self.ue_dependencies_path}")
        src_files.extend(dep_files)
        return src_files

    def update_ue_dependencies(self):
        """
        Run GitDependencies.exe for an UnrealEngine repo.
        """
        git_deps_exe = os.path.join(self.config["source"]["root"], self.git_dependencies_path)
        if not os.path.isfile(git_deps_exe):
            LOG.error(f"GitDependencies.exe not found: {git_deps_exe}")
            sys.exit(1)

        args = [git_deps_exe]
        LOG.info(f"Running {subprocess.list2cmdline(args)}")
        return subprocess.run(args)

    def get_ue_dependencies(self) -> list[GitFile]:
        """
        Return a list of all additional dependency file paths that were downloaded by GitDependencies.exe
        """
        src_root = self.config["source"]["root"]
        deps_file_path = os.path.join(self.config["source"]["root"], self.ue_dependencies_path)
        if not os.path.isfile(deps_file_path):
            LOG.error(f"{deps_file_path} doesn't exist, run Setup.Bat or GitDependencies.exe first")
            sys.exit(1)

        tree = ElementTree.parse(deps_file_path)
        root = tree.getroot()
        paths = [GitFile(src_root, f.get("Name"), False) for f in root.findall(".//File")]
        return paths


@click.group
def cli():
    """
    A tool for mirroring a git branch's latest commit to perforce.
    Purpose built for syncing GitHub UnrealEngine releases to a local p4 server.
    """
    pass


@cli.command()
@click.option("-c", "config_file", default="config.toml")
@click.option("-n", "dry_run", is_flag=True)
def run(config_file: str, dry_run=False):
    """
    Run the harmonize tool to stage all differences in files between
    the git HEAD commit (source) and a perforce stream (destination).
    """
    # peek at the config to see if this is for UnrealEngine
    config = load_toml_config(config_file)
    is_unreal = config["source"].get("is_unreal", False)

    harmonize_cls = P4HarmonizeGitUnreal if is_unreal else P4HarmonizeGit
    p4h = harmonize_cls(config_file, dry_run=dry_run)
    p4h.run()

    if dry_run:
        LOG.info("This was a dry-run.")


@cli.command()
@click.option("-c", "config_file", default="config.toml")
@click.option("-n", "dry_run", is_flag=True)
def clean(config_file: str, dry_run=False):
    """
    Cleanup destination workspaces and files that were auto generated during the last run.
    """
    p4h = P4HarmonizeGit(config_file, dry_run=dry_run)
    p4h.clean()

    if dry_run:
        LOG.info("This was a dry-run.")


if __name__ == "__main__":
    cli()
