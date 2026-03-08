
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataExtractor:
    
    def __init__(self, data_sources_path):
        self.data_sources_path = Path(data_sources_path)
        self.extraction_stats = {}
        
    def extract_csv(self, filename, **kwargs):

        try:
            file_path = self.data_sources_path / filename
            logger.info(f"Extracting data from {filename}...")
            
            df = pd.read_csv(file_path, **kwargs)
            
            # Record statistics
            self.extraction_stats[filename] = {
                'records': len(df),
                'columns': len(df.columns),
                'file_path': str(file_path)
            }
            
            logger.info(f"✅ Extracted {len(df)} records from {filename}")
            return df
            
        except FileNotFoundError:
            logger.error(f"❌ File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"❌ Error extracting {filename}: {e}")
            raise
    
    def extract_sales_data(self):
        """Extract sales transactions data"""
        return self.extract_csv('sales.csv')
    
    def extract_customers_data(self):
        """Extract customers data"""
        return self.extract_csv('customers.csv')
    
    def extract_products_data(self):
        """Extract products data"""
        return self.extract_csv('products.csv')
    
    def extract_stores_data(self):
        """Extract stores data"""
        return self.extract_csv('stores.csv')
    
    def extract_all(self):

        logger.info("=" * 50)
        logger.info("STARTING DATA EXTRACTION")
        logger.info("=" * 50)
        
        data = {
            'sales': self.extract_sales_data(),
            'customers': self.extract_customers_data(),
            'products': self.extract_products_data(),
            'stores': self.extract_stores_data()
        }
        
        logger.info("=" * 50)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 50)
        for source, stats in self.extraction_stats.items():
            logger.info(f"{source}: {stats['records']} records, {stats['columns']} columns")
        
        return data
    
    def get_extraction_stats(self):
        return self.extraction_stats


if __name__ == "__main__":
    # Test extraction
    logging.basicConfig(level=logging.INFO)
    
    extractor = DataExtractor("../data_sources")
    data = extractor.extract_all()
    
    # Display sample data
    for name, df in data.items():
        print(f"\n{name.upper()} - Sample Data:")
        print(df.head(3))
