

import pandas as pd
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

class DataLoader:
    
    def __init__(self, engine):
        self.engine = engine
        self.loading_stats = {}
        
    def load_dim_date(self, start_date, end_date):

        logger.info("Populating date dimension...")
        
        try:
            with self.engine.connect() as conn:
                # Call the stored procedure
                result = conn.execute(
                    text("SELECT populate_date_dimension(:start_date, :end_date)"),
                    {"start_date": start_date, "end_date": end_date}
                )
                conn.commit()
                
            # Count records
            with self.engine.connect() as conn:
                count_result = conn.execute(text("SELECT COUNT(*) FROM dim_date"))
                count = count_result.fetchone()[0]
                
            self.loading_stats['dim_date'] = {'records_inserted': count}
            logger.info(f"✅ Date dimension populated: {count} records")
            
        except Exception as e:
            logger.error(f"❌ Error populating date dimension: {e}")
            raise
    
    def load_dimension_table(self, df, table_name, key_column):

        logger.info(f"Loading {table_name}...")
        initial_count = len(df)
        
        try:
            # Truncate and load pattern for dimension tables
            with self.engine.connect() as conn:
                # Delete existing data
                conn.execute(text(f"DELETE FROM {table_name}"))
                conn.commit()
                
            # Insert data
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            
            # Verify count
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                final_count = result.fetchone()[0]
            
            self.loading_stats[table_name] = {
                'records_inserted': final_count,
                'records_source': initial_count
            }
            
            logger.info(f"✅ {table_name} loaded: {final_count} records")
            
        except Exception as e:
            logger.error(f"❌ Error loading {table_name}: {e}")
            raise
    
    def load_fact_table(self, df, products_df, customers_df, stores_df):

        logger.info("Loading fact_sales...")
        initial_count = len(df)
        
        try:
            # Get surrogate key mappings from database
            with self.engine.connect() as conn:
                # Product keys
                product_keys = pd.read_sql(
                    "SELECT product_id, product_key FROM dim_product", 
                    conn
                )
                # Customer keys
                customer_keys = pd.read_sql(
                    "SELECT customer_id, customer_key FROM dim_customer", 
                    conn
                )
                # Store keys
                store_keys = pd.read_sql(
                    "SELECT store_id, store_key FROM dim_store", 
                    conn
                )
                # Date keys
                date_keys = pd.read_sql(
                    "SELECT full_date, date_key FROM dim_date", 
                    conn
                )
            
            # Map natural keys to surrogate keys
            df = df.merge(product_keys, on='product_id', how='inner')
            df = df.merge(customer_keys, on='customer_id', how='inner')
            df = df.merge(store_keys, on='store_id', how='inner')
            
            # Map date to date_key
            df['order_date'] = pd.to_datetime(df['order_date']).dt.date
            date_keys['full_date'] = pd.to_datetime(date_keys['full_date']).dt.date
            df = df.merge(date_keys, left_on='order_date', right_on='full_date', how='inner')
            
            # Select and rename columns for fact table
            fact_data = df[[
                'product_key', 'customer_key', 'store_key', 'date_key',
                'quantity', 'price', 'revenue', 'cost', 'profit'
            ]].copy()
            
            fact_data.rename(columns={'price': 'unit_price'}, inplace=True)
            
            # Clear existing fact data
            with self.engine.connect() as conn:
                conn.execute(text("DELETE FROM fact_sales"))
                conn.commit()
            
            # Insert fact data
            fact_data.to_sql('fact_sales', self.engine, if_exists='append', index=False)
            
            # Verify count
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM fact_sales"))
                final_count = result.fetchone()[0]
            
            self.loading_stats['fact_sales'] = {
                'records_inserted': final_count,
                'records_source': initial_count
            }
            
            logger.info(f"✅ fact_sales loaded: {final_count} records")
            
        except Exception as e:
            logger.error(f"❌ Error loading fact_sales: {e}")
            raise
    
    def load_all(self, transformed_data, date_range_start, date_range_end):

        logger.info("=" * 50)
        logger.info("STARTING DATA LOADING")
        logger.info("=" * 50)
        
        # Load date dimension first
        self.load_dim_date(date_range_start, date_range_end)
        
        # Load dimension tables
        self.load_dimension_table(
            transformed_data['products'], 
            'dim_product', 
            'product_key'
        )
        self.load_dimension_table(
            transformed_data['customers'], 
            'dim_customer', 
            'customer_key'
        )
        self.load_dimension_table(
            transformed_data['stores'], 
            'dim_store', 
            'store_key'
        )
        
        # Load fact table
        self.load_fact_table(
            transformed_data['sales'],
            transformed_data['products'],
            transformed_data['customers'],
            transformed_data['stores']
        )
        
        logger.info("=" * 50)
        logger.info("LOADING SUMMARY")
        logger.info("=" * 50)
        for table, stats in self.loading_stats.items():
            logger.info(f"{table}: {stats.get('records_inserted', 0)} records loaded")
    
    def get_loading_stats(self):
        """Get loading statistics"""
        return self.loading_stats


if __name__ == "__main__":
    # Test loading
    logging.basicConfig(level=logging.INFO)
    
    import sys
    sys.path.append('../warehouse')
    from db_config import create_db_engine
    
    # Create engine
    engine = create_db_engine()
    
    # Initialize loader
    loader = DataLoader(engine)
    
    # Example: Load date dimension
    from datetime import datetime, timedelta
    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    loader.load_dim_date(start_date, end_date)
