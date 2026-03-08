#!/usr/bin/env python3

import sys
import os
import time
from datetime import datetime, timedelta


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import configuration
from logger_config import setup_logging, get_logger
from warehouse.db_config import create_db_engine, test_connection

# Import ETL modules
from extract.data_extractor import DataExtractor
from transform.data_transformer import DataTransformer
from load.data_loader import DataLoader

# Setup logging
setup_logging()
logger = get_logger(__name__)


class ETLPipeline:

    
    def __init__(self, data_sources_path='data_sources'):
        self.data_sources_path = data_sources_path
        self.engine = None
        self.stats = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'status': 'NOT_STARTED',
            'extract': {},
            'transform': {},
            'load': {}
        }
        
    def initialize(self):
  
        logger.info("=" * 60)
        logger.info("ETL DATA WAREHOUSE PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("-" * 60)
        
        # Test database connection
        logger.info("Initializing database connection...")
        if not test_connection():
            raise Exception("Failed to connect to database")
        
        self.engine = create_db_engine()
        logger.info("✅ Database connection established")
        
    def extract(self):
        """Extract data from sources"""
        logger.info("\n" + "=" * 60)
        logger.info("STEP 1: DATA EXTRACTION")
        logger.info("=" * 60)
        
        extractor = DataExtractor(self.data_sources_path)
        raw_data = extractor.extract_all()
        
        self.stats['extract'] = extractor.get_extraction_stats()
        return raw_data
    
    def transform(self, raw_data):
        """Transform and validate data"""
        logger.info("\n" + "=" * 60)
        logger.info("STEP 2: DATA TRANSFORMATION & VALIDATION")
        logger.info("=" * 60)
        
        transformer = DataTransformer()
        transformed_data = transformer.transform_all(raw_data)
        
        self.stats['transform'] = transformer.get_transformation_stats()
        validation_errors = transformer.get_validation_errors()
        
        if validation_errors:
            logger.warning(f"⚠️  {len(validation_errors)} validation issues found")
        else:
            logger.info("✅ All data validation passed")
            
        return transformed_data
    
    def load(self, transformed_data):

        logger.info("\n" + "=" * 60)
        logger.info("STEP 3: DATA LOADING")
        logger.info("=" * 60)
        
        loader = DataLoader(self.engine)
        
        # Define date range for date dimension
        date_range_start = (datetime.now() - timedelta(days=120)).strftime('%Y-%m-%d')
        date_range_end = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        
        loader.load_all(transformed_data, date_range_start, date_range_end)
        
        self.stats['load'] = loader.get_loading_stats()
    
    def generate_summary_report(self):
        """Generate pipeline execution summary"""
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        
        logger.info(f"Status: {self.stats['status']}")
        logger.info(f"Start Time: {self.stats['start_time']}")
        logger.info(f"End Time: {self.stats['end_time']}")
        logger.info(f"Duration: {self.stats['duration_seconds']:.2f} seconds")
        logger.info("-" * 60)
        
        # Extraction summary
        logger.info("EXTRACTION:")
        for source, stats in self.stats['extract'].items():
            logger.info(f"  {source}: {stats.get('records', 0)} records")
        
        # Transformation summary
        logger.info("TRANSFORMATION:")
        for table, stats in self.stats['transform'].items():
            logger.info(f"  {table}: {stats.get('input_records', 0)} → "
                       f"{stats.get('output_records', 0)} records")
        
        # Loading summary
        logger.info("LOADING:")
        for table, stats in self.stats['load'].items():
            logger.info(f"  {table}: {stats.get('records_inserted', 0)} records loaded")
        
        logger.info("=" * 60)
    
    def run(self):
        """Execute the complete ETL pipeline"""
        self.stats['start_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.stats['status'] = 'RUNNING'
        
        try:
            # Initialize
            self.initialize()
            
            # Execute ETL stages
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            self.load(transformed_data)
            
            # Mark as successful
            self.stats['status'] = 'SUCCESS'
            logger.info("\n✅ ETL Pipeline completed successfully!")
            
        except Exception as e:
            self.stats['status'] = 'FAILED'
            logger.error(f"\n❌ ETL Pipeline failed: {e}")
            raise
            
        finally:
            # Record end time and duration
            self.stats['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            start = datetime.strptime(self.stats['start_time'], '%Y-%m-%d %H:%M:%S')
            end = datetime.strptime(self.stats['end_time'], '%Y-%m-%d %H:%M:%S')
            self.stats['duration_seconds'] = (end - start).total_seconds()
            
            # Generate summary
            self.generate_summary_report()
    
    def get_stats(self):
        """Get pipeline execution statistics"""
        return self.stats


def main():
  
    # Get data sources path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_sources_path = os.path.join(script_dir, 'data_sources')
    
    # Create and run pipeline
    pipeline = ETLPipeline(data_sources_path)
    pipeline.run()
    
    return pipeline.get_stats()


if __name__ == "__main__":
    try:
        stats = main()
        sys.exit(0 if stats['status'] == 'SUCCESS' else 1)
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)
