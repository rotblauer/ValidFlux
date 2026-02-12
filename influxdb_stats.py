#!/usr/bin/env python3
"""
Simple script to list stats from an InfluxDB instance.
Provides information about databases, measurements, and data points.
"""

import argparse
import sys
from influxdb import InfluxDBClient


def get_databases(client):
    """Get list of databases from InfluxDB."""
    try:
        databases = client.get_list_database()
        return [db['name'] for db in databases]
    except Exception as e:
        print(f"Error getting databases: {e}", file=sys.stderr)
        return []


def get_measurements(client, database):
    """Get list of measurements for a database."""
    try:
        client.switch_database(database)
        result = client.query('SHOW MEASUREMENTS')
        measurements = []
        if result:
            for item in result.get_points():
                measurements.append(item['name'])
        return measurements
    except Exception as e:
        print(f"Error getting measurements for {database}: {e}", file=sys.stderr)
        return []


def get_measurement_stats(client, database, measurement):
    """Get statistics for a specific measurement."""
    try:
        client.switch_database(database)
        
        # Get count of points
        count_query = f'SELECT COUNT(*) FROM "{measurement}"'
        count_result = client.query(count_query)
        count = 0
        for point in count_result.get_points():
            # Get the first value from the point (count of first field)
            for key, value in point.items():
                if key != 'time' and value is not None:
                    count = value
                    break
        
        # Get time range
        time_query = f'SELECT * FROM "{measurement}" ORDER BY time ASC LIMIT 1'
        first_result = client.query(time_query)
        first_time = None
        for point in first_result.get_points():
            first_time = point.get('time')
            break
        
        time_query = f'SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1'
        last_result = client.query(time_query)
        last_time = None
        for point in last_result.get_points():
            last_time = point.get('time')
            break
        
        return {
            'count': count,
            'first_time': first_time,
            'last_time': last_time
        }
    except Exception as e:
        print(f"Error getting stats for {measurement}: {e}", file=sys.stderr)
        return None


def print_stats(client, detailed=False):
    """Print statistics for InfluxDB instance."""
    databases = get_databases(client)
    
    if not databases:
        print("No databases found or unable to connect.")
        return
    
    print("=" * 80)
    print("InfluxDB Instance Statistics")
    print("=" * 80)
    print(f"\nTotal Databases: {len(databases)}")
    print("-" * 80)
    
    for db in databases:
        if db.startswith('_'):  # Skip internal databases
            continue
            
        print(f"\nDatabase: {db}")
        measurements = get_measurements(client, db)
        print(f"  Measurements: {len(measurements)}")
        
        if detailed and measurements:
            print(f"  Measurement details:")
            for measurement in measurements:
                stats = get_measurement_stats(client, db, measurement)
                if stats:
                    print(f"    - {measurement}:")
                    print(f"        Points: {stats['count']}")
                    if stats['first_time'] and stats['last_time']:
                        print(f"        Time range: {stats['first_time']} to {stats['last_time']}")
    
    print("\n" + "=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='List statistics from an InfluxDB instance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s --host localhost --port 8086
  %(prog)s --host localhost --user admin --password secret --detailed
  %(prog)s --host influxdb.example.com --database mydb
        '''
    )
    
    parser.add_argument('--host', default='localhost',
                        help='InfluxDB host (default: localhost)')
    parser.add_argument('--port', type=int, default=8086,
                        help='InfluxDB port (default: 8086)')
    parser.add_argument('--user', default='',
                        help='InfluxDB username')
    parser.add_argument('--password', default='',
                        help='InfluxDB password')
    parser.add_argument('--database', default='',
                        help='Specific database to query (default: all databases)')
    parser.add_argument('--detailed', action='store_true',
                        help='Show detailed statistics including measurement counts')
    parser.add_argument('--ssl', action='store_true',
                        help='Use SSL connection')
    
    args = parser.parse_args()
    
    try:
        # Create InfluxDB client
        client = InfluxDBClient(
            host=args.host,
            port=args.port,
            username=args.user,
            password=args.password,
            ssl=args.ssl,
            verify_ssl=args.ssl
        )
        
        # Test connection
        client.ping()
        
        print_stats(client, detailed=args.detailed)
        
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
