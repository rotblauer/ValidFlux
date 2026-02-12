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
    is_manifest_file,
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

    def test_v2_bolt(self):
        self.assertTrue(is_meta_file("bolt"))

    def test_v2_kv(self):
        self.assertTrue(is_meta_file("kv"))

    def test_non_meta(self):
        self.assertFalse(is_meta_file("mydb.autogen.00001.00"))
        self.assertFalse(is_meta_file("manifest"))
        self.assertFalse(is_meta_file("manifest.json"))
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
        self.assertFalse(is_shard_file("bolt"))


class TestIsManifestFile(unittest.TestCase):
    """Tests for the is_manifest_file helper."""

    def test_v2_manifest(self):
        self.assertTrue(is_manifest_file("manifest.json"))

    def test_v1_manifest(self):
        self.assertTrue(is_manifest_file("manifest"))

    def test_portable_manifest(self):
        self.assertTrue(is_manifest_file("20220214T120000Z.manifest"))

    def test_non_manifest(self):
        self.assertFalse(is_manifest_file("bolt"))
        self.assertFalse(is_manifest_file("meta.00"))
        self.assertFalse(is_manifest_file("data.tsm"))


class TestValidateManifest(unittest.TestCase):
    """Tests for the validate_manifest function."""

    def test_valid_manifest_with_files(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"files": ["meta.00", "mydb.autogen.00001.00"]}, f)
            f.flush()
            valid, result = validate_manifest(f.name)
        os.unlink(f.name)
        self.assertTrue(valid)
        self.assertIn("files", result)

    def test_valid_v2_manifest(self):
        """InfluxDB 2.x manifest.json may not have a 'files' key."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"version": 1, "buckets": []}, f)
            f.flush()
            valid, result = validate_manifest(f.name)
        os.unlink(f.name)
        self.assertTrue(valid)

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

    def test_no_files_key(self):
        """A manifest without 'files' produces no missing/found."""
        manifest = {"version": 1}
        missing, found = validate_manifest_files_exist(manifest, {"bolt"})
        self.assertEqual(missing, [])
        self.assertEqual(found, 0)


# ---- Directory-based backup tests ----------------------------------------

class TestValidateBackupDirectoryV1(unittest.TestCase):
    """Tests for validate_backup_directory -- InfluxDB 1.x backups."""

    def _make_legacy_backup(self, tmpdir):
        with open(os.path.join(tmpdir, "meta.00"), 'wb') as f:
            f.write(b'\x00' * 64)
        with open(os.path.join(tmpdir, "mydb.autogen.00001.00"), 'wb') as f:
            f.write(b'\x00' * 128)

    def _make_portable_backup(self, tmpdir):
        ts = "20220214T120000Z"
        manifest = {"files": [f"{ts}.meta", f"{ts}.s1.tar.gz"]}
        with open(os.path.join(tmpdir, f"{ts}.manifest"), 'w') as f:
            json.dump(manifest, f)
        with open(os.path.join(tmpdir, f"{ts}.meta"), 'wb') as f:
            f.write(b'\x00' * 64)
        with open(os.path.join(tmpdir, f"{ts}.s1.tar.gz"), 'wb') as f:
            f.write(b'\x00' * 128)

    def test_valid_legacy_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_legacy_backup(tmpdir)
            self.assertTrue(validate_backup_directory(tmpdir))

    def test_valid_portable_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_portable_backup(tmpdir)
            self.assertTrue(validate_backup_directory(tmpdir))

    def test_missing_meta_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "mydb.autogen.00001.00"), 'wb') as f:
                f.write(b'\x00' * 128)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_empty_meta_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "meta.00"), 'wb') as f:
                pass  # empty
            with open(os.path.join(tmpdir, "mydb.autogen.00001.00"), 'wb') as f:
                f.write(b'\x00' * 128)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_no_data_files_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "meta.00"), 'wb') as f:
                f.write(b'\x00' * 64)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_manifest_missing_file_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ts = "20220214T120000Z"
            manifest = {"files": [f"{ts}.meta", f"{ts}.s1.tar.gz"]}
            with open(os.path.join(tmpdir, f"{ts}.manifest"), 'w') as f:
                json.dump(manifest, f)
            with open(os.path.join(tmpdir, f"{ts}.meta"), 'wb') as f:
                f.write(b'\x00' * 64)
            # Add an unrelated data file so validation reaches the manifest
            # cross-referencing step (which should catch the missing shard).
            with open(os.path.join(tmpdir, "other.autogen.00001.00"), 'wb') as f:
                f.write(b'\x00' * 64)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_nonexistent_dir_fails(self):
        self.assertFalse(validate_backup_directory("/nonexistent/path"))


class TestValidateBackupDirectoryV2(unittest.TestCase):
    """Tests for validate_backup_directory -- InfluxDB 2.x backups."""

    def _make_v2_backup(self, tmpdir):
        """Create a valid InfluxDB 2.x backup directory.

        Layout produced by ``influx backup``:
            manifest.json
            bolt
            <shard-id>/
                000000001.tsm
        """
        manifest = {"version": 2, "files": [
            {"fileName": "bolt"},
            {"fileName": "1234/000000001.tsm"},
        ]}
        with open(os.path.join(tmpdir, "manifest.json"), 'w') as f:
            json.dump(manifest, f)
        with open(os.path.join(tmpdir, "bolt"), 'wb') as f:
            f.write(b'\x00' * 256)
        shard_dir = os.path.join(tmpdir, "1234")
        os.makedirs(shard_dir)
        with open(os.path.join(shard_dir, "000000001.tsm"), 'wb') as f:
            f.write(b'\x00' * 512)

    def test_valid_v2_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_v2_backup(tmpdir)
            self.assertTrue(validate_backup_directory(tmpdir))

    def test_v2_missing_bolt_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "manifest.json"), 'w') as f:
                json.dump({"version": 2}, f)
            shard_dir = os.path.join(tmpdir, "1234")
            os.makedirs(shard_dir)
            with open(os.path.join(shard_dir, "000000001.tsm"), 'wb') as f:
                f.write(b'\x00' * 512)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_v2_empty_bolt_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "manifest.json"), 'w') as f:
                json.dump({"version": 2}, f)
            with open(os.path.join(tmpdir, "bolt"), 'wb') as f:
                pass  # empty
            shard_dir = os.path.join(tmpdir, "1234")
            os.makedirs(shard_dir)
            with open(os.path.join(shard_dir, "000000001.tsm"), 'wb') as f:
                f.write(b'\x00' * 512)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_v2_no_shard_data_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "manifest.json"), 'w') as f:
                json.dump({"version": 2}, f)
            with open(os.path.join(tmpdir, "bolt"), 'wb') as f:
                f.write(b'\x00' * 256)
            self.assertFalse(validate_backup_directory(tmpdir))

    def test_v2_kv_instead_of_bolt(self):
        """Some InfluxDB 2.x versions use 'kv' instead of 'bolt'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "manifest.json"), 'w') as f:
                json.dump({"version": 2}, f)
            with open(os.path.join(tmpdir, "kv"), 'wb') as f:
                f.write(b'\x00' * 256)
            shard_dir = os.path.join(tmpdir, "1234")
            os.makedirs(shard_dir)
            with open(os.path.join(shard_dir, "000000001.tsm"), 'wb') as f:
                f.write(b'\x00' * 512)
            self.assertTrue(validate_backup_directory(tmpdir))

    def test_v2_manifest_cross_ref(self):
        """Manifest references a file that doesn't exist -> fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = {"version": 2, "files": [
                {"fileName": "bolt"},
                {"fileName": "9999/missing.tsm"},
            ]}
            with open(os.path.join(tmpdir, "manifest.json"), 'w') as f:
                json.dump(manifest, f)
            with open(os.path.join(tmpdir, "bolt"), 'wb') as f:
                f.write(b'\x00' * 256)
            shard_dir = os.path.join(tmpdir, "1234")
            os.makedirs(shard_dir)
            with open(os.path.join(shard_dir, "000000001.tsm"), 'wb') as f:
                f.write(b'\x00' * 512)
            self.assertFalse(validate_backup_directory(tmpdir))


# ---- Archive-based backup tests -----------------------------------------

class TestValidateBackupArchive(unittest.TestCase):
    """Tests for validate_backup_archive -- tar-based backups."""

    def _create_tar(self, tmpdir, files, compress=False, root_dir=None):
        """Create a tar archive.

        *files* is a dict {name: content}.  If *root_dir* is given every
        member is prefixed with ``root_dir/`` to simulate ``tar -czf``
        wrapping a directory.
        """
        mode = 'w:gz' if compress else 'w'
        suffix = '.tar.gz' if compress else '.tar'
        archive_path = os.path.join(tmpdir, f"backup{suffix}")
        with tarfile.open(archive_path, mode) as tar:
            if root_dir:
                d = tarfile.TarInfo(name=root_dir)
                d.type = tarfile.DIRTYPE
                tar.addfile(d)
            for name, content in files.items():
                data = content if isinstance(content, bytes) else content.encode()
                full_name = f"{root_dir}/{name}" if root_dir else name
                info = tarfile.TarInfo(name=full_name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        return archive_path

    # -- InfluxDB 1.x archives --

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

    def test_nonexistent_archive_fails(self):
        self.assertFalse(validate_backup_archive("/nonexistent/backup.tar"))

    def test_not_a_tar_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_file = os.path.join(tmpdir, "backup.tar")
            with open(bad_file, 'w') as f:
                f.write("this is not a tar file")
            self.assertFalse(validate_backup_archive(bad_file))

    # -- InfluxDB 2.x archives --

    def test_valid_v2_archive(self):
        """Archive with manifest.json + bolt + shard data."""
        manifest = json.dumps({"version": 2, "files": [
            {"fileName": "bolt"},
            {"fileName": "1234/000000001.tsm"},
        ]})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "bolt": b'\x00' * 256,
                "1234/000000001.tsm": b'\x00' * 512,
            })
            self.assertTrue(validate_backup_archive(archive))

    def test_valid_v2_wrapped_archive(self):
        """Archive produced by ``tar -czf ... -C dir backup_dir``.

        All members are nested under a single top-level directory, exactly
        as the backup_database() shell function creates.
        """
        manifest = json.dumps({"version": 2, "files": [
            {"fileName": "bolt"},
            {"fileName": "1234/000000001.tsm"},
        ]})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "bolt": b'\x00' * 256,
                "1234/000000001.tsm": b'\x00' * 512,
            }, compress=True, root_dir="influxdb_backup_20240101_120000")
            self.assertTrue(validate_backup_archive(archive))

    def test_v2_missing_bolt_fails(self):
        manifest = json.dumps({"version": 2})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "1234/000000001.tsm": b'\x00' * 512,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_v2_empty_bolt_fails(self):
        manifest = json.dumps({"version": 2})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "bolt": b'',
                "1234/000000001.tsm": b'\x00' * 512,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_v2_no_data_fails(self):
        manifest = json.dumps({"version": 2})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "bolt": b'\x00' * 256,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_v2_manifest_cross_ref_fails(self):
        """Manifest references a file not present in archive."""
        manifest = json.dumps({"version": 2, "files": [
            {"fileName": "bolt"},
            {"fileName": "9999/missing.tsm"},
        ]})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "bolt": b'\x00' * 256,
                "1234/000000001.tsm": b'\x00' * 512,
            })
            self.assertFalse(validate_backup_archive(archive))

    def test_v2_wrapped_missing_bolt_fails(self):
        """Wrapped archive without bolt/kv should fail."""
        manifest = json.dumps({"version": 2})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "1234/000000001.tsm": b'\x00' * 512,
            }, root_dir="influxdb_backup_20240101_120000")
            self.assertFalse(validate_backup_archive(archive))

    def test_v2_wrapped_restore_workflow(self):
        """Simulate the exact backup_database() + restore_database() workflow.

        backup_database() runs:
            influx backup "$BACKUP_PATH"
            tar -czf "${BACKUP_PATH}.tar.gz" -C "$BACKUP_DIR" \
                "influxdb_backup_$TIMESTAMP"

        restore_database() runs:
            tar -xzf "$BACKUP_FILE" -C "$TEMP_DIR"
            BACKUP_DATA=$(find "$TEMP_DIR" -type d \
                -name "influxdb_backup_*" | head -1)
            influx restore "$BACKUP_DATA" --token ... --full

        The archive must contain a root dir matching influxdb_backup_*.
        """
        manifest = json.dumps({"version": 2, "files": [
            {"fileName": "bolt"},
            {"fileName": "1234/000000001.tsm"},
        ]})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "bolt": b'\x00' * 256,
                "1234/000000001.tsm": b'\x00' * 512,
            }, compress=True, root_dir="influxdb_backup_20240101_120000")
            self.assertTrue(validate_backup_archive(archive))

    def test_v2_wrong_root_dir_warns(self):
        """Archive with a non-standard root dir name should still validate
        but print a warning that restore script may not find the backup."""
        manifest = json.dumps({"version": 2})
        with tempfile.TemporaryDirectory() as tmpdir:
            archive = self._create_tar(tmpdir, {
                "manifest.json": manifest,
                "bolt": b'\x00' * 256,
                "1234/000000001.tsm": b'\x00' * 512,
            }, root_dir="my_custom_backup_name")
            # Should still pass validation (data is valid) but warn
            self.assertTrue(validate_backup_archive(archive))


if __name__ == '__main__':
    unittest.main()
