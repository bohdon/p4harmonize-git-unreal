import logging
import os.path
import shutil
import sys
import tomllib

import click
import git
from P4 import P4

LOG = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format="%(levelname)8s [%(name)s] %(message)s")


class GitFile(object):
    def __init__(self, root: str, path: str):
        self.root = root
        self.relative_path = path
        self.full_path = os.path.join(self.root, self.relative_path)

    def __repr__(self):
        return f"<GitFile '{self.full_path}'>"

    def __str__(self):
        return self.full_path


class P4File(object):
    depot_path: str


def p4_run(p4: P4, args):
    LOG.info(f"p4 {' '.join(args)}")
    result = p4.run(*args)
    for warning in p4.warnings:
        LOG.warning(warning)
    return result


def load_toml_config(file_path: str) -> dict:
    with open(file_path, "rb") as fp:
        return tomllib.load(fp)


class P4HarmonizeGit(object):
    def __init__(self, config_file: str, dry_run=False):
        self.config = load_toml_config(config_file)
        self._p4 = None
        self.dry_run = dry_run

    @property
    def p4(self):
        if self._p4 is None:
            self._p4 = self.get_p4_connection()
        return self._p4

    def get_p4_connection(self) -> P4:
        p4 = P4()
        dest_config = self.config["destination"]
        p4.port = dest_config["p4port"]
        p4.user = dest_config["p4user"]
        p4.client = dest_config["p4client"]

        p4.connect()
        p4.exception_level = p4.RAISE_ERRORS

        return p4

    def list_git_files(self, git_dir: str) -> list[GitFile]:
        g = git.cmd.Git(git_dir)
        paths = g.ls_tree(["-r", "--name-only", "HEAD"]).split("\n")
        result = [GitFile(git_dir, p) for p in paths]
        return result

    def list_p4_files(self, depot_path: str) -> list[P4File]:
        args = ["fstat", depot_path]
        return p4_run(self.p4, args)

    def create_dest_p4_client(self):
        client = p4_run(self.p4, ["client", "-o", self.config["destination"]["p4client"]])[0]
        client._root = self.config["destination"]["root"]
        client._stream = self.config["destination"]["stream"]
        print(client)
        LOG.info("p4 client -i ...")
        if not self.dry_run:
            self.p4.save_client(client)

    def create_dest_p4_changelist(self, description: str) -> str:
        return "123"

    def delete_p4_client(self, p4client: str):
        clients = p4_run(self.p4, ["clients", "-e", p4client])
        if not clients:
            LOG.info(f"Client not found: {p4client}")
            return
        else:
            LOG.info(f"Deleting client: {p4client}")

        if not self.dry_run:
            p4_run(self.p4, ["client", "-d", p4client])

    def copy_files_to_dest(self, src_files: list[GitFile]) -> list[str]:
        LOG.info(f"Copying {len(src_files)} files to destination workspace...")

        dst_root = self.config["destination"]["root"]
        result = []
        for src_file in src_files:
            dst_file = os.path.join(dst_root, src_file.relative_path)
            if not self.dry_run:
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                shutil.copy2(src_file.full_path, dst_file, follow_symlinks=False)
            result.append(dst_file)

        return result

    def run(self):
        self.run_preflight()

        # list files from source repro
        LOG.info(f"Finding files in source...")
        src_root = self.config["source"]["root"]
        src_files = self.list_git_files(src_root)
        LOG.info(f"{len(src_files)} files in source repro: {src_root}")

        # list files from dest depot
        LOG.info("Finding files in destination...")
        p4_stream = self.config["destination"]["stream"]
        dst_files = self.list_p4_files(f"{p4_stream}/...")
        LOG.info(f"{len(dst_files)} files in the destination depot")
        LOG.info(dst_files[:10])

        # create destination workspace
        self.create_dest_p4_client()
        if not self.dry_run:
            p4_run(self.p4, ["flush", f"{p4_stream}/..."])

        # copy files to dest
        dst_files = self.copy_files_to_dest(src_files)

        # check out files
        if not self.dry_run:
            p4_run(self.p4, ["add"] + dst_files)

    def run_preflight(self):
        dest_root = self.config["destination"]["root"]
        if os.path.isdir(dest_root) and os.listdir(dest_root):
            LOG.error(f"Destination root must be empty: {dest_root}")
            sys.exit(1)

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
    p4h = P4HarmonizeGit(config_file, dry_run=dry_run)
    p4h.run()

    if dry_run:
        print("This was a dry-run.")


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
        print("This was a dry-run.")


if __name__ == "__main__":
    cli()
