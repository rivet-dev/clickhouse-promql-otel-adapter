"""Exercise patch application and failed regeneration without network access."""

import hashlib
import importlib.util
import io
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("regenerate", ROOT / "vendor/promql-parser/regenerate.py")
vendor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(vendor)


class VendorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.archive = self.directory / "test.crate"
        with tarfile.open(self.archive, "w:gz") as crate:
            for name, content in {"src/lib.rs": b"upstream\n", "Cargo.lock": b"upstream lock\n"}.items():
                info = tarfile.TarInfo(f"promql-parser-{vendor.VERSION}/{name}")
                info.size = len(content)
                info.mode = 0o644
                crate.addfile(info, io.BytesIO(content))
        self.patch = self.directory / "compat.patch"
        self.patch.write_text("--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-upstream\n+patched\n")
        self.output = self.directory / "out"
        self.output.mkdir()
        (self.output / "old").write_text("previous generation")
        overrides = patch.multiple(vendor, DIRECTORY=self.directory, PATCH=self.patch,
                                   DESTINATION=self.output,
                                   SHA256=hashlib.sha256(self.archive.read_bytes()).hexdigest())
        overrides.start()
        self.addCleanup(overrides.stop)

    def test_regenerate_and_check(self):
        vendor.regenerate(self.archive)
        self.assertEqual((self.output / "src/lib.rs").read_text(), "patched\n")
        self.assertFalse((self.output / "old").exists())
        self.assertFalse((self.output / "Cargo.lock").exists())
        (self.output / "Cargo.lock").write_text("local lock")
        vendor.regenerate(self.archive, check=True)
        self.assertEqual((self.output / "Cargo.lock").read_text(), "local lock")

    def test_check_rejects_changed_missing_and_extra_files_without_rewriting(self):
        for drift in ("changed", "missing", "extra"):
            with self.subTest(drift=drift):
                vendor.regenerate(self.archive)
                source = self.output / "src/lib.rs"
                if drift == "changed":
                    source.write_text("unreviewed edit\n")
                elif drift == "missing":
                    source.unlink()
                else:
                    (self.output / "extra.rs").write_text("unreviewed file")
                before = vendor.contents(self.output)
                with self.assertRaisesRegex(RuntimeError, "Generated parser differs"):
                    vendor.regenerate(self.archive, check=True)
                self.assertEqual(vendor.contents(self.output), before)

    def test_bad_checksum_preserves_existing_source(self):
        self.archive.write_bytes(b"corrupted")
        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            vendor.regenerate(self.archive)
        self.assertEqual((self.output / "old").read_text(), "previous generation")

    def test_bad_patch_preserves_existing_source(self):
        self.patch.write_text(self.patch.read_text().replace("-upstream", "-different"))
        with self.assertRaises(subprocess.CalledProcessError):
            vendor.regenerate(self.archive)
        self.assertEqual((self.output / "old").read_text(), "previous generation")
        self.assertEqual(list(self.directory.glob(".promql-parser-*")), [])


if __name__ == "__main__":
    unittest.main()
