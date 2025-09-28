# P4 Harmonize for Git UnrealEngine

- A tool for mirroring a git branch's latest commit to perforce.
- Purpose built for syncing [UnrealEngine](https://github.com/EpicGames/UnrealEngine/) releases to a local p4 server.
- Based on [p4harmonize](https://github.com/danbrakeley/p4harmonize) which is a tool for mirroring p4-to-p4.

## Usage

- Create a `config.toml` mapping the source git repo and destination p4 server.
- Set `is_unreal` to True to enable some Unreal-specific behavior:
  - Runs `GitDependencies.exe` and gathers additional source files from `.uedependencies`.
  - Provides default lists of files to ignore, like `.gitattributes`, `Setup.bat`, etc.

```toml
[source]
root = "D:/github/EpicGames/UnrealEngine"
is_unreal = true

[destination]
p4port = "perforce:1666"
p4user = "bsayre"
p4client = "bsayre_my-machine_UE5-harmonize"
stream = "//UE5/Main"
root = "D:/mystudio/p4/UE5-harmonize"
```

- Clone or checkout the desired source branch in the git repo.
- Run the `run` command, starting in the same directory as the config, or pass it via `-c my/config.toml`

```bash
$ python p4harmonize-git-ue.py run
```

- All differences between the git repo and p4 stream will be staged to a changelist.
- Review and submit!

## Cleaning and Re-running

- The destination workspace and root dir shouldn't exist before running.
- Use the `clean` command to delete the workspace and root directory (make sure it's set correctly!).

```bash
$ python p4harmonize-git-ue.py clean
```

## How it works

- Files are listed from source and destination using `git ls-tree` and `p4 fstat`, so untracked files are ignored.
- Additional UnrealEngine files are discovered from `.uedependencies` (stuff downloaded via `Setup.bat`)
- An initial difference is calculated manually:
  - Source or destination only files are easily marked for add/delete
  - Case mismatches are handled as `p4 move` (case-sensitive server) or `p4 delete`, which requires a second run
  - A simple file size check is done for binary files, and then an `md5` digest is calculated and compared against p4's
    known digest.
- Files are copied from git repo -> p4 workspace and staged.
- Unchanged files are reverted with `p4 revert -a`, this also handles any false-positives from the digest checks.

> Nothing is ever submitted automatically, review and submit after running the tool.
