#!/usr/bin/env python3


import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from warehouse.db_config import DB_CONFIG


def create_database():
    """Create the data warehouse database if it doesn't exist"""
    # Connect to default postgres database
    conn_params = DB_CONFIG.copy()
    database_name = conn_params.pop('database')
    conn_params['database'] = 'postgres'
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (database_name,)
        )
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"✅ Database '{database_name}' created successfully")
        else:
            print(f"ℹ️  Database '{database_name}' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False


def create_schema():
    """Create the data warehouse schema"""
    schema_file = os.path.join(os.path.dirname(__file__), 'schema.sql')
    
    if not os.path.exists(schema_file):
        print(f"❌ Schema file not found: {schema_file}")
        return False
    
    try:
        # Read schema SQL
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Connect to the data warehouse database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Execute schema SQL
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✅ Schema created successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating schema: {e}")
        return False


def main():
    """Main setup function"""
    print("=" * 60)
    print("DATA WAREHOUSE DATABASE SETUP")
    print("=" * 60)
    print()
    
    # Create database
    print("Step 1: Creating database...")
    if not create_database():
        sys.exit(1)
    
    # Create schema
    print("\nStep 2: Creating schema...")
    if not create_schema():
        sys.exit(1)
    
    print()
    print("=" * 60)
    print("SETUP COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. Run the ETL pipeline: python main_pipeline.py")
    print("  2. Generate reports: python queries/analytics.py")
    print()


if __name__ == "__main__":
    main()
