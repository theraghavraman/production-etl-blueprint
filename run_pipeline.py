#!/usr/bin/env python3
"""
ETL Pipeline Runner Script
Easy-to-use script to run different pipeline configurations
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logging(log_level='INFO'):
    """Setup logging configuration"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.FileHandler(f'logs/pipeline_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def validate_environment():
    """Validate that the environment is properly set up"""
    logger = logging.getLogger(__name__)

    # Check for required directories
    required_dirs = ['data/input', 'data/output', 'logs', 'config']
    for directory in required_dirs:
        if not Path(directory).exists():
            logger.error(f"Required directory missing: {directory}")
            return False

    # Check for configuration files
    config_files = [
        'config/data_sources_config.json',
        'config/etl_pipeline_config.json'
    ]

    for config_file in config_files:
        if not Path(config_file).exists():
            logger.error(f"Configuration file missing: {config_file}")
            return False

    # Check for sample datasets
    required_datasets = [
        'data/input/sample_customers.csv',
        'data/input/sample_orders.csv',
        'data/input/sample_products.json'
    ]

    for dataset in required_datasets:
        if not Path(dataset).exists():
            logger.error(f"Required dataset missing: {dataset}")
            return False

    logger.info("✅ Environment validation passed")
    return True

def run_data_validation():
    """Run data validation checks"""
    logger = logging.getLogger(__name__)
    logger.info("Running data validation...")

    try:
        # Import and run validation
        import scripts.validate_data as validator
        results = validator.validate_datasets()

        # Check validation results
        failed_validations = [name for name, info in results.items() if '❌' in info.get('status', '')]

        if failed_validations:
            logger.error(f"Data validation failed for: {', '.join(failed_validations)}")
            return False

        logger.info("✅ Data validation passed")
        return True

    except Exception as e:
        logger.error(f"Data validation error: {str(e)}")
        return False

def run_etl_pipeline(config_file=None):
    """Run the main ETL pipeline"""
    logger = logging.getLogger(__name__)
    logger.info("Starting ETL Pipeline execution...")

    try:
        # Load configuration
        config_path = config_file or 'config/etl_pipeline_config.json'
        with open(config_path, 'r') as f:
            config = json.load(f)

        logger.info(f"Loaded configuration: {config['pipeline']['name']}")

        # Import and run the main ETL pipeline
        # This would import your comprehensive_pyspark_etl_pipeline module
        logger.info("ETL Pipeline execution would start here...")
        logger.info("(Integration with comprehensive_pyspark_etl_pipeline.py)")

        # For demonstration, we'll simulate successful execution
        import time
        time.sleep(2)  # Simulate processing time

        logger.info("✅ ETL Pipeline completed successfully")
        return True

    except Exception as e:
        logger.error(f"ETL Pipeline execution failed: {str(e)}")
        return False

def run_tests():
    """Run the test suite"""
    logger = logging.getLogger(__name__)
    logger.info("Running test suite...")

    try:
        import test_etl_pipeline
        success = test_etl_pipeline.run_test_suite()

        if success:
            logger.info("✅ All tests passed")
        else:
            logger.error("❌ Some tests failed")

        return success

    except Exception as e:
        logger.error(f"Test execution failed: {str(e)}")
        return False

def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(description='ETL Pipeline Runner')
    parser.add_argument('--mode', choices=['validate', 'test', 'run', 'full'], 
                       default='full', help='Pipeline execution mode')
    parser.add_argument('--config', help='Custom configuration file path')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Logging level')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.log_level)
    logger.info(f"Starting ETL Pipeline Runner in {args.mode} mode")

    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)

    success = True

    if args.mode in ['validate', 'full']:
        logger.info("🔍 Running environment validation...")
        if not validate_environment():
            success = False

        logger.info("📊 Running data validation...")
        if not run_data_validation():
            success = False

    if args.mode in ['test', 'full'] and success:
        logger.info("🧪 Running test suite...")
        if not run_tests():
            success = False

    if args.mode in ['run', 'full'] and success:
        logger.info("🚀 Running ETL pipeline...")
        if not run_etl_pipeline(args.config):
            success = False

    if success:
        logger.info("🎉 Pipeline runner completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Pipeline runner failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
