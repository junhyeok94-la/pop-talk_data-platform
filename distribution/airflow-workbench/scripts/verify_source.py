"""Verify exported source files without Docker, Airflow, or third-party packages."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re


def verify(root):
    root = Path(root).resolve()
    manifest = json.loads((root / "SOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    if manifest.get("format") != 1 or not isinstance(manifest.get("files"), dict) or not manifest["files"]:
        raise ValueError("Unsupported or empty source manifest")
    for name, expected in manifest["files"].items():
        relative = PurePosixPath(name)
        if not name or relative.is_absolute() or str(relative) != name or any(p in {".", ".."} for p in relative.parts) or any(c in name for c in "\\:\x00"):
            raise ValueError("Unsafe manifest path")
        target = root.joinpath(*relative.parts)
        if any(p.is_symlink() for p in [target, *target.parents] if p != root and root in p.parents):
            raise ValueError("Symlinks are not allowed in source files")
        if not target.resolve().is_relative_to(root):
            raise ValueError("Source file is outside the package")
        if not isinstance(expected, str) or not re.fullmatch(r"[a-f0-9]{64}", expected):
            raise ValueError("Invalid manifest checksum")
        if not target.is_file():
            raise ValueError("Missing source file: " + name)
        if hashlib.sha256(target.read_bytes()).hexdigest() != expected:
            raise ValueError("Modified source file: " + name)
    return len(manifest["files"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        count = verify(args.directory)
    except (OSError, ValueError) as exc:
        parser.exit(1, "Source verification failed: " + str(exc) + "\n")
    print(f"Verified {count} source files. Locally added files are not covered by the manifest.")


if __name__ == "__main__":
    main()
