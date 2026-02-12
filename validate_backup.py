#!/usr/bin/env python3
"""
Script to validate an InfluxDB backup.
Checks the integrity and structure of InfluxDB backup files,
ensuring that a valid backup will result in a valid InfluxDB if used to restore.

Supports both InfluxDB 1.x and 2.x backup formats:
  - 1.x legacy:   meta.00, db.rp.shard.index files
  - 1.x portable: <ts>.manifest, <ts>.meta, <ts>.s<N>.tar.gz
  - 2.x:          manifest.json, bolt/kv, shard dirs with .tsm/.wal files
"""

import argparse
import fnmatch
import os
import re
import sys
import tarfile
import json
from pathlib import Path

# ---------- InfluxDB 1.x patterns ----------
# Legacy format: meta.00, db.rp.shard.index (e.g. mydb.autogen.00001.00)
LEGACY_META_PATTERN = re.compile(r'^meta\.\d+$')
LEGACY_SHARD_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.\d+\.\d+$')
# Portable format: <timestamp>.manifest, <timestamp>.meta, <timestamp>.s<N>.tar.gz
PORTABLE_META_PATTERN = re.compile(r'^.{2,}\.meta$')
PORTABLE_SHARD_PATTERN = re.compile(r'^.+\.s\d+\.tar\.gz$')
PORTABLE_MANIFEST_PATTERN = re.compile(r'^.{2,}\.manifest$')

# ---------- InfluxDB 2.x constants ----------
V2_MANIFEST_NAME = 'manifest.json'
V2_BOLT_NAMES = {'bolt', 'kv'}
# InfluxDB 2.x may produce timestamped bolt files (e.g. 20240212T140100Z.bolt)
V2_BOLT_PATTERN = re.compile(r'^.{2,}\.bolt$')


def is_meta_file(filename):
    """Check if a filename matches a known InfluxDB meta/metadata backup pattern.

    Covers 1.x meta.00 / *.meta, 2.x bolt / kv files, and timestamped
    *.bolt files produced by some InfluxDB 2.x versions.
    """
    basename = os.path.basename(filename)
    if basename in V2_BOLT_NAMES:
        return True
    return bool(LEGACY_META_PATTERN.match(basename) or PORTABLE_META_PATTERN.match(basename)
                or V2_BOLT_PATTERN.match(basename))


def is_shard_file(filename):
    """Check if a filename matches a known InfluxDB shard backup pattern."""
    basename = os.path.basename(filename)
    return bool(LEGACY_SHARD_PATTERN.match(basename) or PORTABLE_SHARD_PATTERN.match(basename))


def is_manifest_file(filename):
    """Check if a filename is a manifest (1.x or 2.x)."""
    basename = os.path.basename(filename)
    if basename == V2_MANIFEST_NAME:
        return True
    if basename == 'manifest':
        return True
    return bool(PORTABLE_MANIFEST_PATTERN.match(basename))


def is_metadata_or_manifest(filename):
    """Return True for files that are metadata/manifest (not user data)."""
    return is_meta_file(filename) or is_manifest_file(filename)


def validate_manifest(manifest_path):
    """Validate a manifest file (1.x with 'files' key or 2.x manifest.json)."""
    try:
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

        if not isinstance(manifest, dict):
            return False, "Manifest is not a JSON object"

        return True, manifest
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON in manifest: {e}"
    except Exception as e:
        return False, f"Error reading manifest: {e}"


def validate_manifest_files_exist(manifest, existing_files):
    """Cross-reference manifest entries with files that actually exist.

    Supports both 1.x manifests (with a top-level 'files' list) and 2.x
    manifest.json (with a 'files' list of objects containing 'fileName').

    Returns (missing_files, found_count).
    """
    manifest_files = manifest.get('files', [])
    missing = []
    found = 0
    for entry in manifest_files:
        # Manifest entries may be strings or dicts with a 'fileName' key
        if isinstance(entry, dict):
            fname = entry.get('fileName', entry.get('filename', ''))
        else:
            fname = str(entry)
        if fname and fname in existing_files:
            found += 1
        elif fname:
            missing.append(fname)
    return missing, found


def _find_backup_root_members(members):
    """Detect whether tar members are nested inside a single top-level directory.

    The ``influx backup`` + ``tar -czf`` workflow wraps the backup directory
    (e.g. ``influxdb_backup_20240101_120000/``) into the archive, so all
    members start with that prefix.  This helper strips the prefix so that
    the remaining validation logic sees the same layout as a flat backup
    directory.

    Returns (prefix, file_members) where *prefix* is the common directory
    prefix (possibly empty) and *file_members* is the list of TarInfo objects
    that are regular files.
    """
    file_members = [m for m in members if m.isfile()]

    # Detect a single common top-level directory
    top_dirs = set()
    for m in members:
        parts = Path(m.name).parts
        if len(parts) > 1:
            top_dirs.add(parts[0])

    if len(top_dirs) == 1:
        prefix = top_dirs.pop()
        # Verify every member lives under that prefix
        all_under = all(m.name == prefix or m.name.startswith(prefix + '/')
                        for m in members)
        if all_under:
            return prefix, file_members

    return '', file_members


def validate_backup_directory(backup_path):
    """Validate a backup directory for restore readiness."""
    print(f"Validating backup directory: {backup_path}")
    print("=" * 80)

    backup_dir = Path(backup_path)

    if not backup_dir.exists():
        print(f"ERROR: Backup directory does not exist: {backup_path}")
        return False

    if not backup_dir.is_dir():
        print(f"ERROR: Path is not a directory: {backup_path}")
        return False

    print(f"✓ Backup directory exists")

    # --- Locate manifest ---------------------------------------------------
    manifest = None
    manifest_path = None
    # 2.x manifest.json
    mj = backup_dir / V2_MANIFEST_NAME
    if mj.exists():
        manifest_path = mj
    else:
        # 1.x plain 'manifest'
        mp = backup_dir / 'manifest'
        if mp.exists():
            manifest_path = mp
        else:
            # 1.x portable *.manifest
            portable = list(backup_dir.glob('*.manifest'))
            if portable:
                manifest_path = portable[0]

    if manifest_path is not None:
        print(f"✓ Manifest file found: {manifest_path.name}")
        valid, result = validate_manifest(manifest_path)
        if valid:
            print(f"✓ Manifest is valid JSON")
            manifest = result
            if 'files' in manifest:
                print(f"  Files listed in manifest: {len(manifest['files'])}")
        else:
            print(f"✗ Manifest validation failed: {result}")
            return False
    else:
        print(f"⚠ No manifest file found (optional for some backup types)")

    # --- Collect files -----------------------------------------------------
    all_top_files = [f for f in backup_dir.iterdir() if f.is_file()]
    all_top_filenames = {f.name for f in all_top_files}

    # Also collect all files recursively with relative paths for cross-ref
    all_relative_paths = set(all_top_filenames)
    for f in backup_dir.rglob('*'):
        if f.is_file():
            all_relative_paths.add(str(f.relative_to(backup_dir)))

    # --- Check for meta/bolt/kv (required for restore) ---------------------
    meta_files = [f for f in all_top_files if is_meta_file(f.name)]
    if meta_files:
        print(f"✓ Metadata file found: {', '.join(f.name for f in meta_files)}")
        for mf in meta_files:
            if mf.stat().st_size == 0:
                print(f"✗ Metadata file is empty: {mf.name}")
                return False
    else:
        print(f"✗ No metadata file found (required for restore)")
        return False

    # --- Count data files (shard dirs, top-level shard files) ---------------
    shard_files = [f for f in all_top_files if is_shard_file(f.name)]
    db_dirs = [d for d in backup_dir.iterdir() if d.is_dir()]

    total_files = 0
    total_size = 0

    if db_dirs:
        user_db_dirs = [d for d in db_dirs
                        if not d.name.startswith('_') and d.name != 'manifest']
        if user_db_dirs:
            print(f"\nData directories found: {len(user_db_dirs)}")
        for db_dir in user_db_dirs:
            db_name = db_dir.name
            files = list(db_dir.rglob('*'))
            data_files = [f for f in files if f.is_file()]
            file_count = len(data_files)
            dir_size = sum(f.stat().st_size for f in data_files)

            empty_files = [f for f in data_files if f.stat().st_size == 0]
            if empty_files:
                print(f"  ⚠ {db_name}: {len(empty_files)} empty data file(s)")

            total_files += file_count
            total_size += dir_size

            print(f"  - {db_name}: {file_count} files, {dir_size / 1024 / 1024:.2f} MB")

    if shard_files:
        shard_size = sum(f.stat().st_size for f in shard_files)
        total_files += len(shard_files)
        total_size += shard_size
        print(f"\nShard backup files found: {len(shard_files)}")
        empty_shards = [f for f in shard_files if f.stat().st_size == 0]
        if empty_shards:
            print(f"  ⚠ {len(empty_shards)} empty shard file(s)")

    print(f"\nTotal data files: {total_files}")
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")

    if total_files == 0:
        print("\n⚠ WARNING: No data files found in backup")
        return False

    # --- Cross-reference manifest with actual files -------------------------
    if manifest is not None and 'files' in manifest:
        missing, found = validate_manifest_files_exist(manifest, all_relative_paths)
        if missing:
            print(f"\n✗ {len(missing)} file(s) listed in manifest are missing from backup:")
            for mf in missing:
                print(f"    - {mf}")
            return False
        else:
            print(f"✓ All {found} manifest entries have matching files")

    print("\n" + "=" * 80)
    print("✓ Backup validation completed successfully — backup is ready for restore")
    return True


def validate_backup_archive(archive_path):
    """Validate a backup archive (tar or tar.gz) for restore readiness."""
    print(f"Validating backup archive: {archive_path}")
    print("=" * 80)

    archive_file = Path(archive_path)

    if not archive_file.exists():
        print(f"ERROR: Archive file does not exist: {archive_path}")
        return False

    if not archive_file.is_file():
        print(f"ERROR: Path is not a file: {archive_path}")
        return False

    print(f"✓ Archive file exists")
    print(f"  Size: {archive_file.stat().st_size / 1024 / 1024:.2f} MB")

    try:
        with tarfile.open(archive_path, 'r:*') as tar:
            print(f"✓ Archive is readable as tar")

            members = tar.getmembers()
            print(f"  Total entries in archive: {len(members)}")

            # Detect wrapping directory produced by tar -czf
            prefix, file_members = _find_backup_root_members(members)
            if prefix:
                print(f"  Archive root directory: {prefix}/")

            # The restore script locates the backup via:
            #   find "$TEMP_DIR" -type d -name "influxdb_backup_*"
            # Warn when the archive does not contain such a directory.
            dir_members = [m for m in members if m.isdir()]
            influx_dirs = [m for m in dir_members
                           if fnmatch.fnmatch(os.path.basename(
                               m.name.rstrip('/')), 'influxdb_backup_*')]
            if not influx_dirs:
                # Also check if the prefix itself matches
                if prefix and fnmatch.fnmatch(prefix, 'influxdb_backup_*'):
                    influx_dirs = [prefix]
            if influx_dirs:
                print(f"✓ Backup root directory matches expected "
                      f"influxdb_backup_* pattern")
            else:
                print(f"⚠ No influxdb_backup_* directory found in archive "
                      f"(restore script may not locate the backup)")

            # Build sets of names for lookup (both full paths and relative)
            all_names = set()
            for m in file_members:
                all_names.add(m.name)
                all_names.add(os.path.basename(m.name))
                if prefix and m.name.startswith(prefix + '/'):
                    all_names.add(m.name[len(prefix) + 1:])

            # Helper to get the "relative" name inside the backup root
            def _relname(member):
                n = member.name
                if prefix and n.startswith(prefix + '/'):
                    return n[len(prefix) + 1:]
                return n

            # --- Manifest --------------------------------------------------
            manifest = None
            manifest_members = [m for m in file_members
                                if is_manifest_file(_relname(m))]
            if manifest_members:
                print(f"✓ Manifest file found in archive")
                try:
                    manifest_f = tar.extractfile(manifest_members[0])
                    if manifest_f is not None:
                        manifest_data = json.load(manifest_f)
                        if not isinstance(manifest_data, dict):
                            print(f"✗ Manifest is not a JSON object")
                            return False
                        manifest = manifest_data
                        if 'files' in manifest:
                            print(f"✓ Manifest is valid JSON with "
                                  f"{len(manifest['files'])} file entries")
                        else:
                            print(f"✓ Manifest is valid JSON")
                except (json.JSONDecodeError, Exception) as e:
                    print(f"✗ Could not parse manifest: {e}")
                    return False
            else:
                print(f"⚠ No manifest file found in archive")

            # --- Metadata (meta/bolt/kv) -----------------------------------
            meta_members = [m for m in file_members
                            if is_meta_file(os.path.basename(m.name))]
            if meta_members:
                print(f"✓ Metadata file found: "
                      f"{', '.join(os.path.basename(m.name) for m in meta_members)}")
                for mm in meta_members:
                    if mm.size == 0:
                        print(f"✗ Metadata file is empty: "
                              f"{os.path.basename(mm.name)}")
                        return False
            else:
                print(f"✗ No metadata file found (required for restore)")
                return False

            # --- Data files ------------------------------------------------
            data_files = [m for m in file_members
                          if not is_metadata_or_manifest(os.path.basename(m.name))]
            print(f"  Data files: {len(data_files)}")

            # Empty data file warning
            empty_members = [m for m in data_files if m.size == 0]
            if empty_members:
                print(f"  ⚠ {len(empty_members)} empty data file(s) in archive")

            # Show directory structure
            dirs = set()
            for member in members:
                rel = _relname(member)
                parts = Path(rel).parts
                if len(parts) > 1:
                    dirs.add(parts[0])

            if dirs:
                print(f"\n  Directories in archive:")
                for d in sorted(dirs):
                    if not d.startswith('_'):
                        sub_prefix = (prefix + '/' + d) if prefix else d
                        files_in_dir = [m for m in members
                                        if m.name.startswith(sub_prefix + '/')
                                        and m.isfile()]
                        print(f"    - {d}: {len(files_in_dir)} files")

            if len(data_files) == 0:
                print("\n⚠ WARNING: No data files found in archive")
                return False

            # --- Cross-reference manifest -----------------------------------
            if manifest is not None and 'files' in manifest:
                missing, found = validate_manifest_files_exist(
                    manifest, all_names)
                if missing:
                    print(f"\n✗ {len(missing)} file(s) listed in manifest are "
                          f"missing from archive:")
                    for mf in missing:
                        print(f"    - {mf}")
                    return False
                else:
                    print(f"✓ All {found} manifest entries have matching files")

    except tarfile.TarError as e:
        print(f"✗ Error reading tar archive: {e}")
        return False
    except Exception as e:
        print(f"✗ Error validating archive: {e}")
        return False

    print("\n" + "=" * 80)
    print("✓ Archive validation completed successfully — backup is ready for restore")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Validate an InfluxDB backup',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s /path/to/backup
  %(prog)s /path/to/backup.tar.gz
  %(prog)s /var/lib/influxdb/backup
        '''
    )
    
    parser.add_argument('backup_path',
                        help='Path to backup directory or archive file')
    
    args = parser.parse_args()
    
    backup_path = Path(args.backup_path)
    
    # Determine if it's a directory or archive
    if backup_path.is_dir():
        success = validate_backup_directory(str(backup_path))
    elif backup_path.is_file():
        # Check if it's an archive
        if backup_path.suffix in ['.tar', '.gz', '.tgz'] or '.tar.' in backup_path.name:
            success = validate_backup_archive(str(backup_path))
        else:
            print(f"ERROR: Unknown file type: {backup_path}")
            print("Expected: directory, .tar, .tar.gz, or .tgz file")
            sys.exit(1)
    else:
        print(f"ERROR: Path does not exist: {backup_path}")
        sys.exit(1)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
