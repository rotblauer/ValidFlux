#!/usr/bin/env python3
"""
Script to validate an InfluxDB backup.
Checks the integrity and structure of InfluxDB backup files,
ensuring that a valid backup will result in a valid InfluxDB if used to restore.
"""

import argparse
import os
import re
import sys
import tarfile
import json
from pathlib import Path

# Patterns for InfluxDB backup files
# Legacy format: meta.00, db.rp.shard.index (e.g. mydb.autogen.00001.00)
LEGACY_META_PATTERN = re.compile(r'^meta\.\d+$')
LEGACY_SHARD_PATTERN = re.compile(r'^.+\..+\.\d+\.\d+$')
# Portable format: <timestamp>.manifest, <timestamp>.meta, <timestamp>.<shard>.tar.gz
PORTABLE_META_PATTERN = re.compile(r'^.+\.meta$')
PORTABLE_SHARD_PATTERN = re.compile(r'^.+\.s\d+\.tar\.gz$')
PORTABLE_MANIFEST_PATTERN = re.compile(r'^.+\.manifest$')


def is_meta_file(filename):
    """Check if a filename matches a known InfluxDB meta backup pattern."""
    basename = os.path.basename(filename)
    return bool(LEGACY_META_PATTERN.match(basename) or PORTABLE_META_PATTERN.match(basename))


def is_shard_file(filename):
    """Check if a filename matches a known InfluxDB shard backup pattern."""
    basename = os.path.basename(filename)
    return bool(LEGACY_SHARD_PATTERN.match(basename) or PORTABLE_SHARD_PATTERN.match(basename))


def validate_manifest(manifest_path):
    """Validate the manifest file."""
    try:
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        
        # Check required fields
        required_fields = ['files']
        for field in required_fields:
            if field not in manifest:
                return False, f"Missing required field: {field}"
        
        return True, manifest
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON in manifest: {e}"
    except Exception as e:
        return False, f"Error reading manifest: {e}"


def validate_manifest_files_exist(manifest, existing_files):
    """Cross-reference manifest entries with files that actually exist.
    
    Returns (missing_files, found_count).
    """
    manifest_files = manifest.get('files', [])
    missing = []
    found = 0
    for entry in manifest_files:
        # Manifest entries may be strings (filenames) or dicts with a 'fileName' key
        if isinstance(entry, dict):
            fname = entry.get('fileName', entry.get('filename', ''))
        else:
            fname = str(entry)
        if fname and fname in existing_files:
            found += 1
        elif fname:
            missing.append(fname)
    return missing, found


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
    
    # Check for manifest file (both plain 'manifest' and portable *.manifest)
    manifest_path = backup_dir / 'manifest'
    has_manifest = manifest_path.exists()
    portable_manifests = list(backup_dir.glob('*.manifest'))
    
    if not has_manifest and portable_manifests:
        manifest_path = portable_manifests[0]
        has_manifest = True
    
    print(f"✓ Backup directory exists")
    
    manifest = None
    if has_manifest:
        print(f"✓ Manifest file found")
        valid, result = validate_manifest(manifest_path)
        if valid:
            print(f"✓ Manifest file is valid")
            manifest = result
            print(f"  Files in manifest: {len(manifest.get('files', []))}")
        else:
            print(f"✗ Manifest validation failed: {result}")
            return False
    else:
        print(f"⚠ No manifest file found (optional for some backup types)")
    
    # Collect all files in the backup directory (top-level)
    all_top_files = [f for f in backup_dir.iterdir() if f.is_file()]
    all_top_filenames = {f.name for f in all_top_files}
    
    # Check for meta backup file (required for restore)
    meta_files = [f for f in all_top_files if is_meta_file(f.name)]
    if meta_files:
        print(f"✓ Meta backup file found: {', '.join(f.name for f in meta_files)}")
        # Verify meta files are non-empty
        for mf in meta_files:
            if mf.stat().st_size == 0:
                print(f"✗ Meta backup file is empty: {mf.name}")
                return False
    else:
        print(f"✗ No meta backup file found (required for restore)")
        return False
    
    # Check for shard data files (top-level, for legacy/portable flat backups)
    shard_files = [f for f in all_top_files if is_shard_file(f.name)]
    
    # Look for database directories (for directory-structured backups)
    db_dirs = [d for d in backup_dir.iterdir() if d.is_dir()]
    
    total_files = 0
    total_size = 0
    
    if db_dirs:
        user_db_dirs = [d for d in db_dirs
                        if not d.name.startswith('_') and d.name != 'manifest']
        if user_db_dirs:
            print(f"\nDatabase directories found: {len(user_db_dirs)}")
        for db_dir in user_db_dirs:
            db_name = db_dir.name
            files = list(db_dir.rglob('*'))
            data_files = [f for f in files if f.is_file()]
            file_count = len(data_files)
            dir_size = sum(f.stat().st_size for f in data_files)
            
            # Check for empty data files
            empty_files = [f for f in data_files if f.stat().st_size == 0]
            if empty_files:
                print(f"  ⚠ {db_name}: {len(empty_files)} empty data file(s)")
            
            total_files += file_count
            total_size += dir_size
            
            print(f"  - {db_name}: {file_count} files, {dir_size / 1024 / 1024:.2f} MB")
    
    # Also count top-level shard files
    if shard_files:
        shard_size = sum(f.stat().st_size for f in shard_files)
        total_files += len(shard_files)
        total_size += shard_size
        print(f"\nShard backup files found: {len(shard_files)}")
        # Check for empty shard files
        empty_shards = [f for f in shard_files if f.stat().st_size == 0]
        if empty_shards:
            print(f"  ⚠ {len(empty_shards)} empty shard file(s)")
    
    print(f"\nTotal data files: {total_files}")
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")
    
    if total_files == 0:
        print("\n⚠ WARNING: No data files found in backup")
        return False
    
    # Cross-reference manifest with actual files
    if manifest is not None:
        missing, found = validate_manifest_files_exist(manifest, all_top_filenames)
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
        # Try to open as tar archive
        with tarfile.open(archive_path, 'r:*') as tar:
            print(f"✓ Archive is readable as tar")
            
            members = tar.getmembers()
            print(f"  Total entries in archive: {len(members)}")
            
            # Build a set of all file names for cross-referencing
            all_member_names = set()
            for m in members:
                all_member_names.add(m.name)
                # Also add the basename for flat-style matching
                all_member_names.add(os.path.basename(m.name))
            
            # Look for manifest
            manifest_members = [m for m in members
                                if m.isfile() and (
                                    os.path.basename(m.name) == 'manifest'
                                    or PORTABLE_MANIFEST_PATTERN.match(os.path.basename(m.name))
                                )]
            manifest = None
            if manifest_members:
                print(f"✓ Manifest file found in archive")
                # Try to parse the manifest
                try:
                    manifest_f = tar.extractfile(manifest_members[0])
                    if manifest_f is not None:
                        manifest_data = json.load(manifest_f)
                        if 'files' in manifest_data:
                            manifest = manifest_data
                            print(f"✓ Manifest is valid JSON with {len(manifest['files'])} file entries")
                        else:
                            print(f"✗ Manifest missing required 'files' field")
                            return False
                except (json.JSONDecodeError, Exception) as e:
                    print(f"✗ Could not parse manifest: {e}")
                    return False
            else:
                print(f"⚠ No manifest file found in archive")
            
            # Check for meta backup file (required for restore)
            meta_members = [m for m in members
                            if m.isfile() and is_meta_file(os.path.basename(m.name))]
            if meta_members:
                print(f"✓ Meta backup file found: "
                      f"{', '.join(os.path.basename(m.name) for m in meta_members)}")
                # Verify meta files are non-empty
                for mm in meta_members:
                    if mm.size == 0:
                        print(f"✗ Meta backup file is empty: {os.path.basename(mm.name)}")
                        return False
            else:
                print(f"✗ No meta backup file found (required for restore)")
                return False
            
            # Count shard / database files
            shard_members = [m for m in members
                             if m.isfile() and is_shard_file(os.path.basename(m.name))]
            
            db_files = [m for m in members
                        if m.isfile()
                        and not is_meta_file(os.path.basename(m.name))
                        and os.path.basename(m.name) != 'manifest'
                        and not PORTABLE_MANIFEST_PATTERN.match(os.path.basename(m.name))]
            print(f"  Database/shard files: {len(db_files)}")
            
            if shard_members:
                print(f"  Shard backup files: {len(shard_members)}")
            
            # Check for empty data files
            empty_members = [m for m in db_files if m.size == 0]
            if empty_members:
                print(f"  ⚠ {len(empty_members)} empty data file(s) in archive")
            
            # Show directory structure
            dirs = set()
            for member in members:
                parts = Path(member.name).parts
                if len(parts) > 1:
                    dirs.add(parts[0])
            
            if dirs:
                print(f"\n  Database directories in archive:")
                for d in sorted(dirs):
                    if not d.startswith('_') and d != 'manifest':
                        db_files_in_dir = [m for m in members if m.name.startswith(d + '/') and m.isfile()]
                        print(f"    - {d}: {len(db_files_in_dir)} files")
            
            if len(db_files) == 0:
                print("\n⚠ WARNING: No database files found in archive")
                return False
            
            # Cross-reference manifest with archive contents
            if manifest is not None:
                missing, found = validate_manifest_files_exist(
                    manifest, all_member_names)
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
