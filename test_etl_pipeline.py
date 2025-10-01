#!/usr/bin/env python3
"""
ETL Pipeline Testing Script
Comprehensive tests for all ETL components
"""

import unittest
import pandas as pd
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

class TestDataExtraction(unittest.TestCase):
    """Test data extraction functionality"""

    def setUp(self):
        self.data_dir = Path("data/input")

    def test_csv_files_exist(self):
        """Test that all required CSV files exist"""
        csv_files = [
            "sample_customers.csv",
            "sample_orders.csv", 
            "sample_sales_analytics.csv",
            "sample_dirty_data.csv"
        ]

        for csv_file in csv_files:
            file_path = self.data_dir / csv_file
            self.assertTrue(file_path.exists(), f"{csv_file} does not exist")

    def test_json_files_exist(self):
        """Test that all required JSON files exist"""
        json_files = [
            "sample_products.json",
            "sample_logs.json",
            "sample_streaming_events.json"
        ]

        for json_file in json_files:
            file_path = self.data_dir / json_file
            self.assertTrue(file_path.exists(), f"{json_file} does not exist")

    def test_csv_data_integrity(self):
        """Test CSV data integrity"""
        customers_df = pd.read_csv(self.data_dir / "sample_customers.csv")

        # Test row count
        self.assertEqual(len(customers_df), 10000, "Customers dataset should have 10,000 rows")

        # Test required columns
        required_columns = ['customer_id', 'first_name', 'last_name', 'email']
        for col in required_columns:
            self.assertIn(col, customers_df.columns, f"Missing column: {col}")

        # Test data types
        self.assertTrue(pd.api.types.is_numeric_dtype(customers_df['customer_id']))

    def test_json_data_integrity(self):
        """Test JSON data integrity"""
        with open(self.data_dir / "sample_products.json", 'r') as f:
            products_data = json.load(f)

        self.assertEqual(len(products_data), 1000, "Products dataset should have 1,000 records")

        # Test required fields in first record
        required_fields = ['product_id', 'product_name', 'category', 'price']
        for field in required_fields:
            self.assertIn(field, products_data[0], f"Missing field: {field}")

class TestDataQuality(unittest.TestCase):
    """Test data quality checks"""

    def setUp(self):
        self.dirty_data = pd.read_csv("data/input/sample_dirty_data.csv")

    def test_null_detection(self):
        """Test null value detection"""
        null_counts = self.dirty_data.isnull().sum()

        # Should have null values in the dirty dataset
        self.assertGreater(null_counts.sum(), 0, "Dirty dataset should contain null values")

    def test_duplicate_detection(self):
        """Test duplicate record detection"""
        initial_count = len(self.dirty_data)
        dedupe_count = len(self.dirty_data.drop_duplicates())

        # Should have duplicates in the dirty dataset
        self.assertGreater(initial_count, dedupe_count, "Dirty dataset should contain duplicates")

    def test_data_validation(self):
        """Test data validation rules"""
        # Test invalid ages
        invalid_ages = self.dirty_data[
            (self.dirty_data['age'] < 0) | 
            (self.dirty_data['age'] > 120)
        ]

        self.assertGreater(len(invalid_ages), 0, "Should detect invalid age values")

class TestTransformations(unittest.TestCase):
    """Test data transformation logic"""

    def test_customer_age_calculation(self):
        """Test customer age calculation"""
        from datetime import datetime, date

        # Sample data
        birth_date = date(1990, 1, 1)
        current_date = date(2025, 1, 1)
        expected_age = 35

        # Calculate age
        actual_age = (current_date - birth_date).days / 365.25

        self.assertAlmostEqual(actual_age, expected_age, places=0)

    def test_customer_segmentation(self):
        """Test customer segmentation logic"""
        # Sample customer data
        total_spent_values = [0, 500, 1500, 5000]
        expected_segments = ['Basic', 'Regular', 'Premium', 'Premium']

        for spent, expected in zip(total_spent_values, expected_segments):
            if spent < 500:
                segment = 'Basic'
            elif spent < 1000:
                segment = 'Regular'
            else:
                segment = 'Premium'

            self.assertEqual(segment, expected, f"Incorrect segment for amount: {spent}")

class TestConfiguration(unittest.TestCase):
    """Test configuration files"""

    def test_config_files_exist(self):
        """Test that configuration files exist"""
        config_files = [
            "config/data_sources_config.json",
            "config/etl_pipeline_config.json", 
            "config/sql_transformations.json"
        ]

        for config_file in config_files:
            self.assertTrue(Path(config_file).exists(), f"{config_file} does not exist")

    def test_config_file_structure(self):
        """Test configuration file structure"""
        with open("config/data_sources_config.json", 'r') as f:
            config = json.load(f)

        # Test required sections
        self.assertIn('data_sources', config)
        self.assertIn('data_quality_rules', config)

        # Test data source structure
        customers_config = config['data_sources']['customers']
        required_keys = ['type', 'path', 'schema']
        for key in required_keys:
            self.assertIn(key, customers_config, f"Missing key in customers config: {key}")

class TestPipelinePerformance(unittest.TestCase):
    """Test pipeline performance metrics"""

    def test_data_loading_performance(self):
        """Test data loading performance"""
        import time

        start_time = time.time()

        # Load sample dataset
        df = pd.read_csv("data/input/sample_customers.csv")

        end_time = time.time()
        load_time = end_time - start_time

        # Should load 10k records in less than 5 seconds
        self.assertLess(load_time, 5.0, "Data loading took too long")
        self.assertEqual(len(df), 10000, "Incorrect number of records loaded")

def run_test_suite():
    """Run the complete test suite"""
    print("🧪 Running ETL Pipeline Test Suite...")
    print("="*50)

    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test cases
    test_classes = [
        TestDataExtraction,
        TestDataQuality, 
        TestTransformations,
        TestConfiguration,
        TestPipelinePerformance
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print("\n" + "="*50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")

    return result.wasSuccessful()

if __name__ == "__main__":
    run_test_suite()
