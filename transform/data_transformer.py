

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataTransformer:

    
    def __init__(self):
        self.transformation_stats = {}
        self.validation_errors = []
        
    def remove_duplicates(self, df, subset=None):
        """Remove duplicate records"""
        before_count = len(df)
        df = df.drop_duplicates(subset=subset, keep='first')
        after_count = len(df)
        removed = before_count - after_count
        if removed > 0:
            logger.info(f"Removed {removed} duplicate records")
        return df
    
    def handle_missing_values(self, df, required_columns):

        initial_count = len(df)
        
        # Remove rows where required columns are null
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in required column: {col}")
        
        df = df.dropna(subset=required_columns)
        final_count = len(df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} rows with missing required values")
        
        return df
    
    def validate_positive_numbers(self, df, columns):

        for col in columns:
            if col in df.columns:
                invalid_count = (df[col] < 0).sum()
                if invalid_count > 0:
                    error_msg = f"Found {invalid_count} negative values in {col}"
                    logger.error(error_msg)
                    self.validation_errors.append(error_msg)
                    # Remove invalid records
                    df = df[df[col] >= 0]
        return df
    
    def transform_products(self, df):

        logger.info("Transforming products data...")
        initial_count = len(df)
        
        # Remove duplicates
        df = self.remove_duplicates(df, subset=['product_id'])
        
        # Handle missing values
        df = self.handle_missing_values(df, ['product_id', 'product_name', 'category'])
        
        # Validate positive costs
        df = self.validate_positive_numbers(df, ['unit_cost'])
        
        # Add effective date and is_current flag
        df['effective_date'] = datetime.now().date()
        df['is_current'] = True
        
        self.transformation_stats['products'] = {
            'input_records': initial_count,
            'output_records': len(df),
            'removed_records': initial_count - len(df)
        }
        
        logger.info(f"✅ Products transformed: {len(df)} records")
        return df
    
    def transform_customers(self, df):

        logger.info("Transforming customers data...")
        initial_count = len(df)
        
        # Remove duplicates
        df = self.remove_duplicates(df, subset=['customer_id'])
        
        # Handle missing values
        df = self.handle_missing_values(df, ['customer_id', 'first_name', 'last_name', 'email'])
        
        # Validate email format (basic check)
        invalid_emails = ~df['email'].str.contains('@', na=False)
        if invalid_emails.sum() > 0:
            logger.warning(f"Found {invalid_emails.sum()} invalid email addresses")
            df.loc[invalid_emails, 'email'] = 'invalid@email.com'
        
        # Add customer segment based on registration date
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        days_since_reg = (datetime.now() - df['registration_date']).dt.days
        df['customer_segment'] = pd.cut(
            days_since_reg,
            bins=[0, 30, 90, 365, float('inf')],
            labels=['New', 'Recent', 'Established', 'Loyal']
        )
        
        self.transformation_stats['customers'] = {
            'input_records': initial_count,
            'output_records': len(df),
            'removed_records': initial_count - len(df)
        }
        
        logger.info(f"✅ Customers transformed: {len(df)} records")
        return df
    
    def transform_stores(self, df):
        """Transform stores data for dim_store"""
        logger.info("Transforming stores data...")
        initial_count = len(df)
        
        # Remove duplicates
        df = self.remove_duplicates(df, subset=['store_id'])
        
        # Handle missing values
        df = self.handle_missing_values(df, ['store_id', 'store_name', 'city', 'region'])
        
        self.transformation_stats['stores'] = {
            'input_records': initial_count,
            'output_records': len(df),
            'removed_records': initial_count - len(df)
        }
        
        logger.info(f"✅ Stores transformed: {len(df)} records")
        return df
    
    def transform_sales(self, df, products_df):
     
        logger.info("Transforming sales data...")
        initial_count = len(df)
        
        # Remove duplicates
        df = self.remove_duplicates(df, subset=['order_id'])
        
        # Handle missing values
        df = self.handle_missing_values(df, ['order_id', 'product_id', 'customer_id', 'store_id'])
        
        # Validate positive numbers
        df = self.validate_positive_numbers(df, ['quantity', 'price'])
        
        # Convert date format
        df['order_date'] = pd.to_datetime(df['order_date'])
        
        # Calculate revenue and cost
        df['revenue'] = df['quantity'] * df['price']
        
        # Merge with products to get unit cost
        df = df.merge(
            products_df[['product_id', 'unit_cost']],
            on='product_id',
            how='left'
        )
        
        df['cost'] = df['quantity'] * df['unit_cost']
        df['profit'] = df['revenue'] - df['cost']
        
        # Drop rows where cost couldn't be determined
        df = df.dropna(subset=['unit_cost'])
        
        self.transformation_stats['sales'] = {
            'input_records': initial_count,
            'output_records': len(df),
            'removed_records': initial_count - len(df)
        }
        
        logger.info(f"✅ Sales transformed: {len(df)} records")
        return df
    
    def transform_all(self, raw_data):

        logger.info("=" * 50)
        logger.info("STARTING DATA TRANSFORMATION")
        logger.info("=" * 50)
        
        # Transform dimension tables first
        products = self.transform_products(raw_data['products'])
        customers = self.transform_customers(raw_data['customers'])
        stores = self.transform_stores(raw_data['stores'])
        
        # Transform fact table (needs products for cost calculation)
        sales = self.transform_sales(raw_data['sales'], products)
        
        logger.info("=" * 50)
        logger.info("TRANSFORMATION SUMMARY")
        logger.info("=" * 50)
        for table, stats in self.transformation_stats.items():
            logger.info(f"{table}: {stats['input_records']} → {stats['output_records']} records "
                       f"(removed: {stats['removed_records']})")
        
        if self.validation_errors:
            logger.warning("Validation errors encountered:")
            for error in self.validation_errors:
                logger.warning(f"  - {error}")
        
        return {
            'products': products,
            'customers': customers,
            'stores': stores,
            'sales': sales
        }
    
    def get_transformation_stats(self):

        return self.transformation_stats
    
    def get_validation_errors(self):
        """Get list of validation errors"""
        return self.validation_errors


if __name__ == "__main__":
    # Test transformation
    logging.basicConfig(level=logging.INFO)
    
    # Load raw data
    import sys
    sys.path.append('../extract')
    from data_extractor import DataExtractor
    
    extractor = DataExtractor("../data_sources")
    raw_data = extractor.extract_all()
    
    # Transform data
    transformer = DataTransformer()
    transformed_data = transformer.transform_all(raw_data)
    
    # Display sample
    for name, df in transformed_data.items():
        print(f"\n{name.upper()} - Transformed Sample:")
        print(df.head(3))
