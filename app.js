// ETL Pipeline Website JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // Initialize all components
    initializeNavigation();
    initializeCodeExamples();
    initializePerformanceChart();
    initializeCollapsibleCodeBlocks();
    initializeSQLToggle();
});

// Navigation smooth scrolling
function initializeNavigation() {
    const navLinks = document.querySelectorAll('.main-nav a[href^="#"]');
    
    navLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('href').substring(1);
            const targetElement = document.getElementById(targetId);
            
            if (targetElement) {
                targetElement.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
}

// Load and display code examples
async function initializeCodeExamples() {
    // Load PySpark code
    try {
        const pysparkResponse = await fetch('https://ppl-ai-code-interpreter-files.s3.amazonaws.com/web/direct-files/7cd481b6c8d3ea616c25d448a92588f6/295e59a7-0684-42db-9639-6a4d114cf674/f24f898b.py');
        const pysparkCode = await pysparkResponse.text();
        document.getElementById('pyspark-code').textContent = pysparkCode;
    } catch (error) {
        document.getElementById('pyspark-code').textContent = `# PySpark ETL Pipeline Example
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("ETL_Pipeline") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Extract: Read from various sources
def extract_data():
    # Read from SQL Server
    sql_server_df = spark.read \\
        .format("jdbc") \\
        .option("url", "jdbc:sqlserver://localhost:1433;databaseName=sales") \\
        .option("dbtable", "orders") \\
        .option("user", "username") \\
        .option("password", "password") \\
        .load()
    
    # Read from CSV files
    csv_df = spark.read \\
        .option("header", "true") \\
        .option("inferSchema", "true") \\
        .csv("hdfs://path/to/customer_data.csv")
    
    return sql_server_df, csv_df

# Transform: Clean and enrich data
def transform_data(orders_df, customers_df):
    # Data quality checks
    orders_clean = orders_df.filter(col("order_total") > 0) \\
        .filter(col("customer_id").isNotNull())
    
    # Join datasets
    enriched_df = orders_clean.join(customers_df, "customer_id", "inner")
    
    # Add calculated columns
    final_df = enriched_df.withColumn("order_year", year(col("order_date"))) \\
        .withColumn("customer_segment", 
                   when(col("total_spent") > 1000, "Premium")
                   .when(col("total_spent") > 500, "Gold")
                   .otherwise("Standard"))
    
    return final_df

# Load: Write to data warehouse
def load_data(df):
    df.write \\
        .mode("overwrite") \\
        .format("jdbc") \\
        .option("url", "jdbc:postgresql://localhost:5432/warehouse") \\
        .option("dbtable", "fact_sales") \\
        .option("user", "username") \\
        .option("password", "password") \\
        .save()

# Main ETL process
def main():
    # Extract
    orders_df, customers_df = extract_data()
    
    # Transform
    final_df = transform_data(orders_df, customers_df)
    
    # Load
    load_data(final_df)
    
    spark.stop()

if __name__ == "__main__":
    main()`;
    }

    // Load Airflow DAG code
    try {
        const airflowResponse = await fetch('https://ppl-ai-code-interpreter-files.s3.amazonaws.com/web/direct-files/7cd481b6c8d3ea616c25d448a92588f6/bff6ca8c-601b-49d3-94b4-7aad246c219a/8ebbbe88.py');
        const airflowCode = await airflowResponse.text();
        document.getElementById('airflow-code').textContent = airflowCode;
    } catch (error) {
        document.getElementById('airflow-code').textContent = `# Airflow ETL DAG Example
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'etl_pipeline_dag',
    default_args=default_args,
    description='Daily ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

def extract_task():
    # Extraction logic here
    print("Extracting data from source systems...")
    return "extraction_complete"

def transform_task():
    # Transformation logic here
    print("Transforming data...")
    return "transformation_complete"

def load_task():
    # Loading logic here
    print("Loading data to warehouse...")
    return "load_complete"

# Define tasks
extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    dag=dag
)

# Data quality check
quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /opt/airflow/scripts/quality_check.py',
    dag=dag
)

# Set task dependencies
extract >> transform >> load >> quality_check`;
    }

    // Load Docker configuration
    try {
        const dockerResponse = await fetch('https://ppl-ai-code-interpreter-files.s3.amazonaws.com/web/direct-files/7cd481b6c8d3ea616c25d448a92588f6/bff6ca8c-601b-49d3-94b4-7aad246c219a/5b8b83e4.txt');
        const dockerCode = await dockerResponse.text();
        document.getElementById('docker-code').textContent = dockerCode;
    } catch (error) {
        document.getElementById('docker-code').textContent = `# Docker Compose for ETL Pipeline
version: '3.8'

services:
  spark-master:
    image: bitnami/spark:3.4
    container_name: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ./data:/opt/bitnami/spark/data
      - ./scripts:/opt/bitnami/spark/scripts

  spark-worker:
    image: bitnami/spark:3.4
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
    depends_on:
      - spark-master
    volumes:
      - ./data:/opt/bitnami/spark/data
      - ./scripts:/opt/bitnami/spark/scripts

  postgresql:
    image: postgres:13
    container_name: postgres-warehouse
    environment:
      POSTGRES_DB: warehouse
      POSTGRES_USER: etl_user
      POSTGRES_PASSWORD: secure_password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-scripts:/docker-entrypoint-initdb.d

  airflow-webserver:
    image: apache/airflow:2.7.0
    container_name: airflow-webserver
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    ports:
      - "8081:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
    depends_on:
      - postgresql

volumes:
  postgres_data:`;
    }

    // Load case study markdown
    try {
        const caseStudyResponse = await fetch('https://ppl-ai-code-interpreter-files.s3.amazonaws.com/web/direct-files/7cd481b6c8d3ea616c25d448a92588f6/2370292c-1b1c-4684-8ee6-02386333d1a6/e23474b5.md');
        const caseStudyCode = await caseStudyResponse.text();
        document.getElementById('case-study-md').textContent = caseStudyCode;
    } catch (error) {
        document.getElementById('case-study-md').textContent = `# TechCorp Analytics Case Study

## Challenge
TechCorp Analytics faced the challenge of migrating daily sales data from SQL Server to PostgreSQL while maintaining strict SLA requirements.

## Solution Architecture
- **Source**: SQL Server with 5M daily records
- **Processing**: PySpark-based ETL pipeline
- **Destination**: PostgreSQL data warehouse
- **Orchestration**: Apache Airflow
- **Monitoring**: Prometheus + Grafana

## Implementation Steps

### 1. Infrastructure Setup
- Deployed containerized environment using Docker
- Set up Airflow for workflow orchestration
- Configured monitoring stack

### 2. Data Pipeline Development
- Built PySpark ETL jobs for extraction, transformation, and loading
- Implemented data quality checks and validation
- Added error handling and retry mechanisms

### 3. Performance Optimization
- Implemented partitioning strategies
- Optimized Spark configurations
- Added caching for frequently accessed data

### 4. Monitoring & Alerting
- Set up Grafana dashboards for real-time monitoring
- Configured alerts for pipeline failures
- Implemented data quality monitoring

## Results
- **67% improvement** in processing time
- **$50,000 annual cost savings**
- **$2M additional revenue** from faster insights
- **99.9% data accuracy** achieved and maintained

## Lessons Learned
1. Proper partitioning is crucial for performance
2. Monitoring and alerting prevent data issues
3. Incremental loading reduces processing time
4. Data quality checks are essential for trust`;
    }

    // Trigger syntax highlighting
    if (typeof Prism !== 'undefined') {
        Prism.highlightAll();
    }
}

// Initialize performance chart
function initializePerformanceChart() {
    const ctx = document.getElementById('performanceChart');
    if (!ctx) return;

    // Performance metrics data
    const performanceData = {
        labels: ['Data Extraction Time', 'Transformation Time', 'Data Loading Time', 'Total Pipeline Time', 'Memory Usage', 'Data Accuracy'],
        datasets: [{
            label: 'Before Optimization',
            data: [45, 90, 30, 165, 32, 98.5],
            backgroundColor: '#1FB8CD',
            borderColor: '#1FB8CD',
            borderWidth: 1
        }, {
            label: 'After Optimization',
            data: [15, 30, 10, 55, 16, 99.9],
            backgroundColor: '#FFC185',
            borderColor: '#FFC185',
            borderWidth: 1
        }]
    };

    new Chart(ctx, {
        type: 'bar',
        data: performanceData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'ETL Pipeline Performance: Before vs After Optimization'
                },
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Values'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Metrics'
                    }
                }
            },
            elements: {
                bar: {
                    borderRadius: 4
                }
            }
        }
    });
}

// Initialize collapsible code blocks
function initializeCollapsibleCodeBlocks() {
    const codeBlocks = document.querySelectorAll('.collapsible');
    
    codeBlocks.forEach(block => {
        // Initially collapse code blocks
        block.classList.add('collapsed');
        
        block.addEventListener('click', function() {
            this.classList.toggle('collapsed');
        });
        
        // Also handle keyboard interaction
        block.addEventListener('keydown', function(e) {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                this.classList.toggle('collapsed');
            }
        });
    });
}

// Initialize SQL code toggle
function initializeSQLToggle() {
    const showSqlBtn = document.getElementById('show-sql-btn');
    const sqlCodeBlock = document.querySelector('#sql-code').parentElement;
    
    if (showSqlBtn && sqlCodeBlock) {
        showSqlBtn.addEventListener('click', function() {
            sqlCodeBlock.classList.toggle('hidden');
            
            if (sqlCodeBlock.classList.contains('hidden')) {
                this.textContent = 'Show Example';
            } else {
                this.textContent = 'Hide Example';
                // Trigger syntax highlighting for the newly visible code
                if (typeof Prism !== 'undefined') {
                    Prism.highlightElement(document.getElementById('sql-code'));
                }
            }
        });
    }
}

// Utility function to handle smooth scrolling for older browsers
function smoothScroll(target) {
    const targetPosition = target.offsetTop - 80; // Account for fixed header
    const startPosition = window.pageYOffset;
    const distance = targetPosition - startPosition;
    const duration = 1000;
    let start = null;

    function animation(currentTime) {
        if (start === null) start = currentTime;
        const timeElapsed = currentTime - start;
        const run = ease(timeElapsed, startPosition, distance, duration);
        window.scrollTo(0, run);
        if (timeElapsed < duration) requestAnimationFrame(animation);
    }

    function ease(t, b, c, d) {
        t /= d / 2;
        if (t < 1) return c / 2 * t * t + b;
        t--;
        return -c / 2 * (t * (t - 2) - 1) + b;
    }

    requestAnimationFrame(animation);
}

// Add loading states for code examples
function showLoadingState(elementId) {
    const element = document.getElementById(elementId);
    if (element) {
        element.textContent = '# Loading code example...';
        element.style.fontStyle = 'italic';
        element.style.color = '#666';
    }
}

// Remove loading states
function removeLoadingState(elementId) {
    const element = document.getElementById(elementId);
    if (element) {
        element.style.fontStyle = 'normal';
        element.style.color = '';
    }
}