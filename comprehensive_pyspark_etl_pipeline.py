
# ===================================================================
# COMPREHENSIVE PYSPARK ETL PIPELINE
# SQL Server to PostgreSQL with Performance Benchmarks
# ===================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
import time
import pandas as pd
from datetime import datetime
import os

# ===================================================================
# 1. SPARK SESSION CONFIGURATION
# ===================================================================

def create_spark_session(app_name="ETL_Pipeline"):
    """
    Create optimized Spark session with performance configurations
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # Set log level to reduce verbose output
    spark.sparkContext.setLogLevel("WARN")

    logging.info(f"Spark Session created: {spark.version}")
    return spark

# ===================================================================
# 2. DATA EXTRACTION MODULE
# ===================================================================

class DataExtractor:
    """
    Data extraction from multiple sources with error handling
    """

    def __init__(self, spark_session):
        self.spark = spark_session

    def extract_from_sql_server(self, server, database, table, username, password):
        """
        Extract data from SQL Server with connection pooling
        """
        try:
            start_time = time.time()

            jdbc_url = f"jdbc:sqlserver://{server}:1433;databaseName={database}"

            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table) \
                .option("user", username) \
                .option("password", password) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .option("fetchsize", "10000") \
                .option("numPartitions", "4") \
                .load()

            end_time = time.time()
            logging.info(f"SQL Server extraction completed in {end_time - start_time:.2f} seconds")
            logging.info(f"Records extracted: {df.count()}")

            return df

        except Exception as e:
            logging.error(f"Error extracting from SQL Server: {str(e)}")
            raise

    def extract_from_csv(self, file_path, schema=None):
        """
        Extract data from CSV files with schema inference or predefined schema
        """
        try:
            start_time = time.time()

            if schema:
                df = self.spark.read \
                    .option("header", "true") \
                    .schema(schema) \
                    .csv(file_path)
            else:
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(file_path)

            end_time = time.time()
            logging.info(f"CSV extraction completed in {end_time - start_time:.2f} seconds")

            return df

        except Exception as e:
            logging.error(f"Error extracting from CSV: {str(e)}")
            raise

    def extract_from_json(self, file_path, multiline=True):
        """
        Extract data from JSON files
        """
        try:
            start_time = time.time()

            if multiline:
                df = self.spark.read \
                    .option("multiline", "true") \
                    .json(file_path)
            else:
                df = self.spark.read.json(file_path)

            end_time = time.time()
            logging.info(f"JSON extraction completed in {end_time - start_time:.2f} seconds")

            return df

        except Exception as e:
            logging.error(f"Error extracting from JSON: {str(e)}")
            raise

# ===================================================================
# 3. DATA TRANSFORMATION MODULE
# ===================================================================

class DataTransformer:
    """
    Data transformation operations with performance monitoring
    """

    def __init__(self, spark_session):
        self.spark = spark_session

    def data_quality_check(self, df, table_name):
        """
        Comprehensive data quality assessment
        """
        try:
            start_time = time.time()

            # Basic statistics
            total_rows = df.count()
            total_columns = len(df.columns)

            # Null value analysis
            null_counts = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / total_rows) * 100
                null_counts[column] = {
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2)
                }

            # Duplicate analysis
            duplicate_count = total_rows - df.dropDuplicates().count()

            quality_report = {
                'table_name': table_name,
                'total_rows': total_rows,
                'total_columns': total_columns,
                'duplicate_rows': duplicate_count,
                'null_analysis': null_counts,
                'timestamp': datetime.now().isoformat()
            }

            end_time = time.time()
            logging.info(f"Data quality check completed in {end_time - start_time:.2f} seconds")

            return quality_report

        except Exception as e:
            logging.error(f"Error in data quality check: {str(e)}")
            raise

    def clean_data(self, df, cleaning_rules):
        """
        Apply data cleaning rules
        """
        try:
            start_time = time.time()
            cleaned_df = df

            # Remove duplicates if specified
            if cleaning_rules.get('remove_duplicates', False):
                initial_count = cleaned_df.count()
                cleaned_df = cleaned_df.dropDuplicates()
                final_count = cleaned_df.count()
                logging.info(f"Removed {initial_count - final_count} duplicate rows")

            # Handle null values
            if 'null_handling' in cleaning_rules:
                null_rules = cleaning_rules['null_handling']

                # Fill null values
                if 'fill_values' in null_rules:
                    cleaned_df = cleaned_df.fillna(null_rules['fill_values'])

                # Drop rows with nulls in specific columns
                if 'drop_null_columns' in null_rules:
                    cleaned_df = cleaned_df.dropna(subset=null_rules['drop_null_columns'])

            # Data type conversions
            if 'type_conversions' in cleaning_rules:
                for column, data_type in cleaning_rules['type_conversions'].items():
                    cleaned_df = cleaned_df.withColumn(column, col(column).cast(data_type))

            # Custom transformations
            if 'custom_transformations' in cleaning_rules:
                for transformation in cleaning_rules['custom_transformations']:
                    cleaned_df = transformation(cleaned_df)

            end_time = time.time()
            logging.info(f"Data cleaning completed in {end_time - start_time:.2f} seconds")

            return cleaned_df

        except Exception as e:
            logging.error(f"Error in data cleaning: {str(e)}")
            raise

    def enrich_data(self, df, enrichment_rules):
        """
        Add calculated columns and business logic
        """
        try:
            start_time = time.time()
            enriched_df = df

            # Add calculated columns
            if 'calculated_columns' in enrichment_rules:
                for column_name, expression in enrichment_rules['calculated_columns'].items():
                    enriched_df = enriched_df.withColumn(column_name, expression)

            # Add window functions
            if 'window_functions' in enrichment_rules:
                for func_config in enrichment_rules['window_functions']:
                    window_spec = Window.partitionBy(func_config['partition_by']) \
                                        .orderBy(func_config['order_by'])
                    enriched_df = enriched_df.withColumn(
                        func_config['column_name'],
                        func_config['function'].over(window_spec)
                    )

            # Join with reference data
            if 'joins' in enrichment_rules:
                for join_config in enrichment_rules['joins']:
                    enriched_df = enriched_df.join(
                        join_config['reference_df'],
                        join_config['join_condition'],
                        join_config.get('join_type', 'inner')
                    )

            end_time = time.time()
            logging.info(f"Data enrichment completed in {end_time - start_time:.2f} seconds")

            return enriched_df

        except Exception as e:
            logging.error(f"Error in data enrichment: {str(e)}")
            raise

    def aggregate_data(self, df, aggregation_rules):
        """
        Perform aggregations based on business requirements
        """
        try:
            start_time = time.time()

            aggregated_df = df.groupBy(aggregation_rules['group_by']) \
                             .agg(aggregation_rules['aggregations'])

            end_time = time.time()
            logging.info(f"Data aggregation completed in {end_time - start_time:.2f} seconds")

            return aggregated_df

        except Exception as e:
            logging.error(f"Error in data aggregation: {str(e)}")
            raise

# ===================================================================
# 4. DATA LOADING MODULE
# ===================================================================

class DataLoader:
    """
    Data loading to various destinations with performance optimization
    """

    def __init__(self, spark_session):
        self.spark = spark_session

    def load_to_postgresql(self, df, server, database, table, username, password, 
                          write_mode="append", batch_size=10000):
        """
        Load data to PostgreSQL with batch processing
        """
        try:
            start_time = time.time()

            jdbc_url = f"jdbc:postgresql://{server}:5432/{database}"

            # Optimize for bulk loading
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table) \
                .option("user", username) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", str(batch_size)) \
                .option("numPartitions", "4") \
                .mode(write_mode) \
                .save()

            end_time = time.time()
            logging.info(f"PostgreSQL loading completed in {end_time - start_time:.2f} seconds")

        except Exception as e:
            logging.error(f"Error loading to PostgreSQL: {str(e)}")
            raise

    def load_to_parquet(self, df, output_path, partition_columns=None, compression="snappy"):
        """
        Save data as Parquet files with partitioning
        """
        try:
            start_time = time.time()

            writer = df.write \
                      .mode("overwrite") \
                      .option("compression", compression)

            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            writer.parquet(output_path)

            end_time = time.time()
            logging.info(f"Parquet save completed in {end_time - start_time:.2f} seconds")

        except Exception as e:
            logging.error(f"Error saving to Parquet: {str(e)}")
            raise

    def load_to_csv(self, df, output_path, header=True, delimiter=","):
        """
        Save data as CSV files
        """
        try:
            start_time = time.time()

            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("header", str(header).lower()) \
              .option("delimiter", delimiter) \
              .csv(output_path)

            end_time = time.time()
            logging.info(f"CSV save completed in {end_time - start_time:.2f} seconds")

        except Exception as e:
            logging.error(f"Error saving to CSV: {str(e)}")
            raise

# ===================================================================
# 5. PERFORMANCE MONITORING MODULE
# ===================================================================

class PerformanceMonitor:
    """
    Monitor and benchmark ETL pipeline performance
    """

    def __init__(self):
        self.metrics = {}

    def start_timer(self, operation_name):
        """Start timing an operation"""
        self.metrics[operation_name] = {'start_time': time.time()}

    def end_timer(self, operation_name, record_count=None):
        """End timing an operation and calculate metrics"""
        if operation_name in self.metrics:
            end_time = time.time()
            duration = end_time - self.metrics[operation_name]['start_time']

            self.metrics[operation_name].update({
                'end_time': end_time,
                'duration_seconds': duration,
                'record_count': record_count,
                'records_per_second': record_count / duration if record_count else None
            })

    def get_performance_report(self):
        """Generate comprehensive performance report"""
        report = {
            'pipeline_metrics': self.metrics,
            'total_pipeline_time': sum(
                metric.get('duration_seconds', 0) 
                for metric in self.metrics.values()
            ),
            'timestamp': datetime.now().isoformat()
        }
        return report

# ===================================================================
# 6. MAIN ETL PIPELINE ORCHESTRATOR
# ===================================================================

class ETLPipeline:
    """
    Main ETL pipeline orchestrator
    """

    def __init__(self, config):
        self.config = config
        self.spark = create_spark_session(config.get('app_name', 'ETL_Pipeline'))
        self.extractor = DataExtractor(self.spark)
        self.transformer = DataTransformer(self.spark)
        self.loader = DataLoader(self.spark)
        self.monitor = PerformanceMonitor()

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def run_pipeline(self):
        """
        Execute the complete ETL pipeline
        """
        try:
            logging.info("Starting ETL Pipeline execution")

            # 1. Data Extraction
            self.monitor.start_timer('extraction')
            source_data = self._extract_data()
            self.monitor.end_timer('extraction', source_data.count())

            # 2. Data Quality Assessment
            self.monitor.start_timer('quality_check')
            quality_report = self.transformer.data_quality_check(source_data, 'source_data')
            logging.info(f"Data Quality Report: {quality_report}")
            self.monitor.end_timer('quality_check')

            # 3. Data Transformation
            self.monitor.start_timer('transformation')
            transformed_data = self._transform_data(source_data)
            self.monitor.end_timer('transformation', transformed_data.count())

            # 4. Data Loading
            self.monitor.start_timer('loading')
            self._load_data(transformed_data)
            self.monitor.end_timer('loading', transformed_data.count())

            # 5. Performance Report
            performance_report = self.monitor.get_performance_report()
            logging.info(f"Performance Report: {performance_report}")

            logging.info("ETL Pipeline execution completed successfully")

            return {
                'status': 'success',
                'quality_report': quality_report,
                'performance_report': performance_report
            }

        except Exception as e:
            logging.error(f"ETL Pipeline failed: {str(e)}")
            return {'status': 'failed', 'error': str(e)}

        finally:
            self.spark.stop()

    def _extract_data(self):
        """Extract data from configured sources"""
        source_config = self.config.get('source', {})

        if source_config.get('type') == 'sql_server':
            return self.extractor.extract_from_sql_server(
                server=source_config['server'],
                database=source_config['database'],
                table=source_config['table'],
                username=source_config['username'],
                password=source_config['password']
            )
        elif source_config.get('type') == 'csv':
            return self.extractor.extract_from_csv(source_config['file_path'])
        elif source_config.get('type') == 'json':
            return self.extractor.extract_from_json(source_config['file_path'])
        else:
            raise ValueError("Unsupported source type")

    def _transform_data(self, df):
        """Apply all configured transformations"""
        transform_config = self.config.get('transformations', {})

        # Data cleaning
        if 'cleaning_rules' in transform_config:
            df = self.transformer.clean_data(df, transform_config['cleaning_rules'])

        # Data enrichment
        if 'enrichment_rules' in transform_config:
            df = self.transformer.enrich_data(df, transform_config['enrichment_rules'])

        # Data aggregation
        if 'aggregation_rules' in transform_config:
            df = self.transformer.aggregate_data(df, transform_config['aggregation_rules'])

        return df

    def _load_data(self, df):
        """Load data to configured destinations"""
        destination_config = self.config.get('destination', {})

        if destination_config.get('type') == 'postgresql':
            self.loader.load_to_postgresql(
                df=df,
                server=destination_config['server'],
                database=destination_config['database'],
                table=destination_config['table'],
                username=destination_config['username'],
                password=destination_config['password'],
                write_mode=destination_config.get('write_mode', 'append')
            )
        elif destination_config.get('type') == 'parquet':
            self.loader.load_to_parquet(
                df=df,
                output_path=destination_config['output_path'],
                partition_columns=destination_config.get('partition_columns'),
                compression=destination_config.get('compression', 'snappy')
            )
        elif destination_config.get('type') == 'csv':
            self.loader.load_to_csv(
                df=df,
                output_path=destination_config['output_path']
            )

# ===================================================================
# 7. EXAMPLE USAGE AND CONFIGURATION
# ===================================================================

def create_sample_config():
    """
    Create a sample configuration for the ETL pipeline
    """
    return {
        'app_name': 'Customer_Data_ETL',
        'source': {
            'type': 'sql_server',
            'server': 'localhost',
            'database': 'source_db',
            'table': 'customers',
            'username': 'sa',
            'password': 'password123'
        },
        'transformations': {
            'cleaning_rules': {
                'remove_duplicates': True,
                'null_handling': {
                    'fill_values': {'phone': 'Unknown', 'email': 'no-email@domain.com'},
                    'drop_null_columns': ['customer_id', 'name']
                },
                'type_conversions': {
                    'customer_id': 'integer',
                    'registration_date': 'timestamp'
                }
            },
            'enrichment_rules': {
                'calculated_columns': {
                    'customer_age': datediff(current_date(), col('birth_date')) / 365,
                    'is_premium': when(col('total_spent') > 1000, True).otherwise(False)
                }
            }
        },
        'destination': {
            'type': 'postgresql',
            'server': 'localhost',
            'database': 'target_db',
            'table': 'processed_customers',
            'username': 'postgres',
            'password': 'password123',
            'write_mode': 'overwrite'
        }
    }

# Example of running the pipeline
if __name__ == "__main__":
    # Create configuration
    config = create_sample_config()

    # Initialize and run pipeline
    pipeline = ETLPipeline(config)
    result = pipeline.run_pipeline()

    print(f"Pipeline execution result: {result}")
