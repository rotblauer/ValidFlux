# ValidFlux
Simple helpers to validate influxdbs

## Overview

ValidFlux provides two simple Python scripts to work with InfluxDB:

1. **influxdb_stats.py** - List statistics from an InfluxDB instance
2. **validate_backup.py** - Validate InfluxDB backup files and archives

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### InfluxDB Stats

List statistics from an InfluxDB instance:

```bash
# Basic usage (localhost)
python influxdb_stats.py

# Connect to remote host
python influxdb_stats.py --host influxdb.example.com --port 8086

# With authentication
python influxdb_stats.py --host localhost --user admin --password secret

# Detailed stats (includes measurement counts and time ranges)
python influxdb_stats.py --detailed

# With SSL
python influxdb_stats.py --host localhost --ssl
```

**Options:**
- `--host` - InfluxDB host (default: localhost)
- `--port` - InfluxDB port (default: 8086)
- `--user` - Username for authentication
- `--password` - Password for authentication
- `--database` - Specific database to query (default: all databases)
- `--detailed` - Show detailed statistics including point counts and time ranges
- `--ssl` - Use SSL connection

### Validate Backup

Validate an InfluxDB backup directory or archive:

```bash
# Validate a backup directory
python validate_backup.py /path/to/backup

# Validate a backup archive
python validate_backup.py /path/to/backup.tar.gz

# Verbose output
python validate_backup.py --verbose /path/to/backup
```

**Options:**
- `backup_path` - Path to backup directory or archive file (required)
- `--verbose` - Show verbose output

The script validates:
- Backup directory/archive existence and readability
- Manifest file (if present)
- Database directories and files
- File counts and sizes

## Examples

### Get quick stats from local InfluxDB:
```bash
python influxdb_stats.py
```

### Get detailed stats from remote InfluxDB:
```bash
python influxdb_stats.py --host 192.168.1.100 --user admin --password mypass --detailed
```

### Validate a backup directory:
```bash
python validate_backup.py /var/lib/influxdb/backup
```

### Validate a backup archive:
```bash
python validate_backup.py /backups/influxdb-2024-01-01.tar.gz
```

## Requirements

- Python 3.6+
- influxdb-python library

## License

MIT 
