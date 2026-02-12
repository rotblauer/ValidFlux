#!/usr/bin/env python3
"""
Script to validate an InfluxDB backup.
Checks the integrity and structure of InfluxDB backup files.
"""

import argparse
import os
import sys
import tarfile
import json
from pathlib import Path


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


def validate_backup_directory(backup_path):
    """Validate a backup directory."""
    print(f"Validating backup directory: {backup_path}")
    print("=" * 80)
    
    backup_dir = Path(backup_path)
    
    if not backup_dir.exists():
        print(f"ERROR: Backup directory does not exist: {backup_path}")
        return False
    
    if not backup_dir.is_dir():
        print(f"ERROR: Path is not a directory: {backup_path}")
        return False
    
    # Check for manifest file
    manifest_path = backup_dir / 'manifest'
    has_manifest = manifest_path.exists()
    
    print(f"✓ Backup directory exists")
    
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
        manifest = None
    
    # Look for database directories
    db_dirs = [d for d in backup_dir.iterdir() if d.is_dir()]
    print(f"\nDatabase directories found: {len(db_dirs)}")
    
    total_files = 0
    total_size = 0
    
    for db_dir in db_dirs:
        db_name = db_dir.name
        
        # Skip internal directories
        if db_name.startswith('_') or db_name == 'manifest':
            continue
        
        # Count files in this database directory
        files = list(db_dir.rglob('*'))
        file_count = len([f for f in files if f.is_file()])
        dir_size = sum(f.stat().st_size for f in files if f.is_file())
        
        total_files += file_count
        total_size += dir_size
        
        print(f"  - {db_name}: {file_count} files, {dir_size / 1024 / 1024:.2f} MB")
    
    print(f"\nTotal files: {total_files}")
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")
    
    if total_files == 0:
        print("\n⚠ WARNING: No data files found in backup")
        return False
    
    print("\n" + "=" * 80)
    print("✓ Backup validation completed successfully")
    return True


def validate_backup_archive(archive_path):
    """Validate a backup archive (tar or tar.gz)."""
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
            print(f"  Total files in archive: {len(members)}")
            
            # Look for manifest
            manifest_members = [m for m in members if 'manifest' in m.name]
            if manifest_members:
                print(f"✓ Manifest file found in archive")
            else:
                print(f"⚠ No manifest file found in archive")
            
            # Count database files
            db_files = [m for m in members if m.isfile() and not m.name.endswith('manifest')]
            print(f"  Database files: {len(db_files)}")
            
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
    
    except tarfile.TarError as e:
        print(f"✗ Error reading tar archive: {e}")
        return False
    except Exception as e:
        print(f"✗ Error validating archive: {e}")
        return False
    
    print("\n" + "=" * 80)
    print("✓ Archive validation completed successfully")
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
  %(prog)s --verbose /var/lib/influxdb/backup
        '''
    )
    
    parser.add_argument('backup_path',
                        help='Path to backup directory or archive file')
    parser.add_argument('--verbose', action='store_true',
                        help='Show verbose output')
    
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
