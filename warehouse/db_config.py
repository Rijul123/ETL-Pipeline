
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'etl_datawarehouse'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

def get_connection_string():
    """Generate PostgreSQL connection string"""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def create_db_engine():
    """Create SQLAlchemy engine for database operations"""
    connection_string = get_connection_string()
    engine = create_engine(
        connection_string,
        poolclass=NullPool,
        echo=False
    )
    return engine

def test_connection():
    """Test database connection"""
    try:
        engine = create_db_engine()
        with engine.connect() as conn:
            result = conn.execute("SELECT version()")
            version = result.fetchone()[0]
            print(f"✅ Database connection successful!")
            print(f"PostgreSQL Version: {version}")
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()
