#!/usr/bin/env python3
"""Tests for validate_backup.py"""

import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path

from validate_backup import (
    is_meta_file,
    is_shard_file,
    validate_manifest,
    validate_manifest_files_exist,
    validate_backup_directory,
    validate_backup_archive,
)


class TestIsMetaFile(unittest.TestCase):
    """Tests for the is_meta_file helper."""

    def test_legacy_meta(self):
        self.assertTrue(is_meta_file("meta.00"))
        self.assertTrue(is_meta_file("meta.01"))

    def test_portable_meta(self):
        self.assertTrue(is_meta_file("20220214T120000Z.meta"))

    def test_non_meta(self):
        self.assertFalse(is_meta_file("mydb.autogen.00001.00"))
        self.assertFalse(is_meta_file("manifest"))
        self.assertFalse(is_meta_file("data.tar.gz"))


class TestIsShardFile(unittest.TestCase):
    """Tests for the is_shard_file helper."""

    def test_legacy_shard(self):
        self.assertTrue(is_shard_file("mydb.autogen.00001.00"))
        self.assertTrue(is_shard_file("telegraf.default.00003.02"))

    def test_portable_shard(self):
        self.assertTrue(is_shard_file("20220214T120000Z.s1.tar.gz"))
        self.assertTrue(is_shard_file("20220214T120000Z.s123.tar.gz"))

    def test_non_shard(self):
        self.assertFalse(is_shard_file("meta.00"))
        self.assertFalse(is_shard_file("manifest"))


class TestValidateManifest(unittest.TestCase):
    """Tests for the validate_manifest function."""

    def test_valid_manifest(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"files": ["meta.00", "mydb.autogen.00001.00"]}, f)
            f.flush()
            valid, result = validate_manifest(f.name)
        os.unlink(f.name)
        self.assertTrue(valid)
        self.assertIn("files", result)

    def test_missing_files_field(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"version": 1}, f)
            f.flush()
            valid, result = validate_manifest(f.name)
        os.unlink(f.name)
        self.assertFalse(valid)
        self.assertIn("Missing required field", result)

    def test_invalid_json(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not json {{{")
            f.flush()
            valid, result = validate_manifest(f.name)
        os.unlink(f.name)
        self.assertFalse(valid)
        self.assertIn("Invalid JSON", result)


class TestValidateManifestFilesExist(unittest.TestCase):
    """Tests for the validate_manifest_files_exist function."""

    def test_all_present(self):
        manifest = {"files": ["meta.00", "mydb.autogen.00001.00"]}
        existing = {"meta.00", "mydb.autogen.00001.00"}
        missing, found = validate_manifest_files_exist(manifest, existing)
        self.assertEqual(missing, [])
        self.assertEqual(found, 2)

    def test_missing_file(self):
        manifest = {"files": ["meta.00", "mydb.autogen.00001.00"]}
        existing = {"meta.00"}
        missing, found = validate_manifest_files_exist(manifest, existing)
        self.assertEqual(missing, ["mydb.autogen.00001.00"])
        self.assertEqual(found, 1)

    def test_dict_entries(self):
        manifest = {"files": [
            {"fileName": "meta.00"},
            {"fileName": "mydb.autogen.00001.00"},
        ]}
        existing = {"meta.00", "mydb.autogen.00001.00"}
        missing, found = validate_manifest_files_exist(manifest, existing)
        self.assertEqual(missing, [])
        self.assertEqual(found, 2)

    def test_dict_entries_lowercase(self):
        manifest = {"files": [
            {"filename": "meta.00"},
        ]}
        existing = {"meta.00"}
        missing, found = validate_manifest_files_exist(manifest, existing)
        self.assertEqual(missing, [])
        self.assertEqual(found, 1)


class TestValidateBackupDirectory(unittest.TestCase):
    """Tests for validate_backup_directory — directory-based backups."""

    def _make_legacy_backup(self, tmpdir):
        """Create a valid legacy-format backup directory."""
        # Meta file
        meta = os.path.join(tmpdir, "meta.00")
        with open(meta, 'wb') as f:
            f.write(b'\x00' * 64)

        # Shard files
        shard = os.path.join(tmpdir, "mydb.autogen.00001.00")
        with open(shard, 'wb') as f:
            f.write(b'\x00' * 128)

        return tmpdir

    def _make_portable_backup(self, tmpdir):
        """Create a valid portable-format backup directory."""
        ts = "20220214T120000Z"
        # Manifest
        manifest = {"files": [f"{ts}.meta", f"{ts}.s1.tar.gz"]}
        with open(os.path.join(tmpdir, f"{ts}.manifest"), 'w') as f:
            json.dump(manifest, f)
        # Meta
        with open(os.path.join(tmpdir, f"{ts}.meta"), 'wb') as f:
            f.write(b'\x00' * 64)
        # Shard
        with open(os.path.join(tmpdir, f"{ts}.s1.tar.gz"), 'wb') as f:
            f.write(b'\x00' * 128)
        return tmpdir

    def test_valid_legacy_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_legacy_backup(tmpdir)
            self.assertTrue(validate_backup_directory(tmpdir))

    def test_valid_portable_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_portable_backup(tmpdir)
            self.assertTrue(validate_backup_directory(tmpdir))

    def test_missing_meta_fails(self):
        """A backup without a meta file should fail validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Only create a shard file, no meta
            with open(os.path.join(tmpdir, "mydb.autogen.00001.00"), 'wb') as f:
                f.write(b'\x00' * 128)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_empty_meta_fails(self):
        """A backup with an empty meta file should fail validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "meta.00"), 'wb') as f:
                pass  # empty file
            with open(os.path.join(tmpdir, "mydb.autogen.00001.00"), 'wb') as f:
                f.write(b'\x00' * 128)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_no_data_files_fails(self):
        """A backup with only meta but no data files should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "meta.00"), 'wb') as f:
                f.write(b'\x00' * 64)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_manifest_missing_file_fails(self):
        """A backup where manifest references a non-existent file should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ts = "20220214T120000Z"
            # Manifest references a shard file that doesn't exist
            manifest = {"files": [f"{ts}.meta", f"{ts}.s1.tar.gz"]}
            with open(os.path.join(tmpdir, f"{ts}.manifest"), 'w') as f:
                json.dump(manifest, f)
            with open(os.path.join(tmpdir, f"{ts}.meta"), 'wb') as f:
                f.write(b'\x00' * 64)
            # Note: intentionally NOT creating the shard file
            # Still need at least one data file for the "total_files == 0" check
            # But the manifest cross-ref should catch the missing shard
            # The meta file alone doesn't count as a data file in the
            # directory logic since it isn't a shard, so total_files=0 will
            # trigger first. Let's add an unrelated data file.
            with open(os.path.join(tmpdir, "other.autogen.00001.00"), 'wb') as f:
                f.write(b'\x00' * 64)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_nonexistent_dir_fails(self):
        self.assertFalse(validate_backup_directory("/nonexistent/path"))

    def test_directory_structured_backup(self):
        """A backup with database sub-directories should pass when meta is present."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Meta file
            with open(os.path.join(tmpdir, "meta.00"), 'wb') as f:
                f.write(b'\x00' * 64)
            # Database sub-directory with data files
            db_dir = os.path.join(tmpdir, "mydb")
            os.makedirs(db_dir)
            with open(os.path.join(db_dir, "datafile.tsm"), 'wb') as f:
                f.write(b'\x00' * 256)
            self.assertTrue(validate_backup_directory(tmpdir))


class TestValidateBackupArchive(unittest.TestCase):
    """Tests for validate_backup_archive — tar-based backups."""

    def _create_tar(self, tmpdir, files, compress=False):
        """Create a tar archive with the given files dict {name: content}."""
        mode = 'w:gz' if compress else 'w'
        suffix = '.tar.gz' if compress else '.tar'
        archive_path = os.path.join(tmpdir, f"backup{suffix}")
        with tarfile.open(archive_path, mode) as tar:
            for name, content in files.items():
                data = content if isinstance(content, bytes) else content.encode()
                info = tarfile.TarInfo(name=name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        return archive_path

    def test_valid_legacy_archive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "meta.00": b'\x00' * 64,
                "mydb.autogen.00001.00": b'\x00' * 128,
            })
            self.assertTrue(validate_backup_archive(archive))

    def test_valid_portable_archive(self):
        ts = "20220214T120000Z"
        manifest = json.dumps({"files": [f"{ts}.meta", f"{ts}.s1.tar.gz"]})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                f"{ts}.manifest": manifest,
                f"{ts}.meta": b'\x00' * 64,
                f"{ts}.s1.tar.gz": b'\x00' * 128,
            })
            self.assertTrue(validate_backup_archive(archive))

    def test_valid_compressed_archive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "meta.00": b'\x00' * 64,
                "mydb.autogen.00001.00": b'\x00' * 128,
            }, compress=True)
            self.assertTrue(validate_backup_archive(archive))

    def test_missing_meta_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "mydb.autogen.00001.00": b'\x00' * 128,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_empty_meta_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "meta.00": b'',
                "mydb.autogen.00001.00": b'\x00' * 128,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_no_data_files_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "meta.00": b'\x00' * 64,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_manifest_missing_file_fails(self):
        ts = "20220214T120000Z"
        manifest = json.dumps({"files": [f"{ts}.meta", f"{ts}.s1.tar.gz"]})
        with tempfile.TemporaryDirectory() as tmpdir:
            # Manifest references s1.tar.gz but we don't include it
            archive = self._create_tar(tmpdir, {
                f"{ts}.manifest": manifest,
                f"{ts}.meta": b'\x00' * 64,
                "mydb.autogen.00001.00": b'\x00' * 128,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_invalid_manifest_json_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "20220214T120000Z.manifest": "not json {{{",
                "20220214T120000Z.meta": b'\x00' * 64,
                "mydb.autogen.00001.00": b'\x00' * 128,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_manifest_missing_files_field_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "20220214T120000Z.manifest": json.dumps({"version": 1}),
                "20220214T120000Z.meta": b'\x00' * 64,
                "mydb.autogen.00001.00": b'\x00' * 128,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_nonexistent_archive_fails(self):
        self.assertFalse(validate_backup_archive("/nonexistent/backup.tar"))

    def test_not_a_tar_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_file = os.path.join(tmpdir, "backup.tar")
            with open(bad_file, 'w') as f:
                f.write("this is not a tar file")
            self.assertFalse(validate_backup_archive(bad_file))


if __name__ == '__main__':
    unittest.main()
