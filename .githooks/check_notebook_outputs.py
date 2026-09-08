"""Fail if any notebook in the outgoing commits still has cell outputs.

Run as a pre-push hook. Git supplies one line per ref being pushed on stdin:

    <local ref> <local sha> <remote ref> <remote sha>
"""

import json
import subprocess
import sys

ZERO = "0" * 40


def git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def outgoing_notebooks(local_sha: str, remote_sha: str) -> dict[str, str]:
    """Blob sha -> path, for every .ipynb this push would send."""
    if remote_sha.strip("0") == "":
        # The remote does not have this branch yet: everything not already on a remote is new.
        listing = git("rev-list", "--objects", local_sha, "--not", "--remotes")
    else:
        listing = git("rev-list", "--objects", f"{remote_sha}..{local_sha}")

    found = {}
    for line in listing.splitlines():
        sha, _, path = line.partition(" ")
        if path.endswith(".ipynb"):
            # Keyed by blob sha, so one notebook touched by ten commits is inspected once.
            found[sha] = path
    return found


def has_outputs(blob_sha: str) -> bool:
    raw = subprocess.run(["git", "cat-file", "-p", blob_sha], capture_output=True).stdout
    try:
        cells = json.loads(raw).get("cells", [])
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False  # not a notebook we can read; not our business to block it
    return any(c.get("outputs") or c.get("execution_count") is not None for c in cells)


def main() -> int:
    offenders = []
    bases = set()
    for line in sys.stdin:
        parts = line.split()
        if len(parts) != 4:
            continue
        _local_ref, local_sha, _remote_ref, remote_sha = parts
        if local_sha.strip("0") == "":
            continue  # branch deletion, nothing to inspect
        for blob_sha, path in outgoing_notebooks(local_sha, remote_sha).items():
            if has_outputs(blob_sha):
                offenders.append((blob_sha, path))
                bases.add(remote_sha if remote_sha.strip("0") else "")

    if not offenders:
        return 0

    # A new commit cannot undo an old blob: the outputs are already inside the commits being
    # pushed, so the fix is to rewrite them, not to add a cleaning commit on top.
    base = bases.pop() if len(bases) == 1 and any(bases) else ""
    reset_target = base[:12] if base else "<the last commit you already pushed>"

    print("\nPush rejected: these committed notebooks still contain outputs.\n", file=sys.stderr)
    for blob_sha, path in sorted(offenders, key=lambda o: o[1]):
        print(f"  {blob_sha[:12]}  {path}", file=sys.stderr)
    print(
        "\nThe nbstripout filter was not active when these were committed, so the outputs are\n"
        "already inside the commits. Pushing would publish them permanently.\n"
        "\n"
        "Set the filter up once:\n"
        "\n"
        "  uv sync && uv run nbstripout --install\n"
        "\n"
        "Then rebuild the commits (nothing here is pushed yet, so this is safe):\n"
        "\n"
        f"  git reset --soft {reset_target}\n"
        "  git add --renormalize .\n"
        '  git commit -m "your message"\n'
        "\n"
        "To push anyway: git push --no-verify\n",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
