#!/usr/bin/env python3
"""Rebuild the committed parser from a checksummed release and reviewed patch."""

import argparse
import hashlib
import io
from pathlib import Path
import subprocess
import tarfile
import tempfile
import urllib.request

VERSION = "0.11.0"
SHA256 = "29df78fc279e9c4acc1cfbdf5624dc51f3df9d83cea59a67e4632e7502d15aa7"
DIRECTORY = Path(__file__).resolve().parent
PATCH = DIRECTORY / "patches" / f"promql-parser-{VERSION}-prometheus-compat.patch"
DESTINATION = DIRECTORY / "out"
URL = f"https://static.crates.io/crates/promql-parser/promql-parser-{VERSION}.crate"


def contents(directory):
    # Cargo may create these locally; they are not part of the generated source.
    return {
        path.relative_to(directory).as_posix(): (path.read_bytes(), path.stat().st_mode & 0o111)
        for path in directory.rglob("*")
        if path.is_file()
        and "target" not in path.relative_to(directory).parts
        and path.relative_to(directory).as_posix() not in ("Cargo.lock", ".cargo-ok")
    }


def regenerate(archive=None, check=False):
    if archive is None:
        with urllib.request.urlopen(URL, timeout=60) as response:
            data = response.read()
    else:
        data = archive.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != SHA256:
        raise RuntimeError(f"Archive checksum mismatch: expected {SHA256}, got {actual}")

    # Stage the entire result before replacing the previous working source.
    with tempfile.TemporaryDirectory(prefix=".promql-parser-", dir=DIRECTORY) as temporary:
        stage = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as crate:
            crate.extractall(stage, filter="data")
        source = stage / f"promql-parser-{VERSION}"
        subprocess.run(
            ["patch", "--batch", "--forward", "--fuzz=0", "-p1"],
            input=PATCH.read_bytes(), cwd=source, check=True,
        )
        (source / "Cargo.lock").unlink(missing_ok=True)
        if check:
            expected, current = contents(source), contents(DESTINATION)
            changed = sorted(name for name in expected.keys() | current.keys()
                             if expected.get(name) != current.get(name))
            if changed:
                raise RuntimeError("Generated parser differs: " + ", ".join(changed)
                                   + ". Edit the patch, then run just vendor-regenerate.")
            print("Vendored parser matches its pinned release and patch.")
            return
        previous = stage / "previous"
        if DESTINATION.exists():
            DESTINATION.rename(previous)
        try:
            source.rename(DESTINATION)
        except OSError:
            if previous.exists():
                previous.rename(DESTINATION)
            raise
        print(f"Regenerated {DESTINATION}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, help="use a local .crate archive (checksum still required)")
    parser.add_argument("--check", action="store_true", help="verify generated files without changing them")
    args = parser.parse_args()
    try:
        regenerate(args.archive, args.check)
    except (OSError, RuntimeError, subprocess.CalledProcessError, tarfile.TarError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
