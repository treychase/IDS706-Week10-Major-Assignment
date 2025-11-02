"""
Super Mario Maker Data Pipeline DAG

This DAG processes Super Mario Maker gaming data (courses and player records)
with parallel ingestion, transformation, merging, and analysis.

Dataset: Super Mario Maker (SMMnet) from Kaggle
- courses.csv: Game level/course data
- records.csv: Player completion records
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import json
from pathlib import Path

# Database connection
DB_CONN = 'postgresql://airflow:airflow@postgres:5432/airflow'

# Directory paths
DATA_DIR = Path('/opt/airflow/data')
PROCESSED_DIR = DATA_DIR / 'processed'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Super Mario Maker ETL Pipeline with parallel processing',
    schedule_interval='@daily',
    catchup=False,
    tags=['gaming', 'etl', 'super_mario_maker'],
)


def setup_directories(**context):
    """Create necessary directories for data processing"""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Directories created: {PROCESSED_DIR}")
    return str(PROCESSED_DIR)


def ingest_courses(**context):
    """Ingest Super Mario Maker courses data"""
    try:
        input_path = DATA_DIR / 'courses.csv'
        
        if not input_path.exists():
            raise FileNotFoundError(f"courses.csv not found at {input_path}")
        
        df = pd.read_csv(input_path)
        print(f"Ingested courses: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Save ingested data
        output_path = PROCESSED_DIR / 'courses_ingested.csv'
        df.to_csv(output_path, index=False)
        
        # Push file path to XCom (not data!)
        context['ti'].xcom_push(key='courses_path', value=str(output_path))
        return str(output_path)
        
    except Exception as e:
        print(f"Error ingesting courses: {str(e)}")
        raise


def ingest_records(**context):
    """Ingest Super Mario Maker player records data"""
    try:
        input_path = DATA_DIR / 'records.csv'
        
        if not input_path.exists():
            raise FileNotFoundError(f"records.csv not found at {input_path}")
        
        df = pd.read_csv(input_path)
        print(f"Ingested records: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Save ingested data
        output_path = PROCESSED_DIR / 'records_ingested.csv'
        df.to_csv(output_path, index=False)
        
        # Push file path to XCom (not data!)
        context['ti'].xcom_push(key='records_path', value=str(output_path))
        return str(output_path)
        
    except Exception as e:
        print(f"Error ingesting records: {str(e)}")
        raise


def transform_courses(**context):
    """Transform and clean courses data"""
    ti = context['task_instance']
    courses_path = ti.xcom_pull(task_ids='ingest_courses', key='courses_path')
    
    df = pd.read_csv(courses_path)
    
    # Handle missing values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df[col].fillna(df[col].median(), inplace=True)
    
    # Standardize string columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip().str.upper()
        df[col].fillna('UNKNOWN', inplace=True)
    
    # Add processing timestamp
    df['processed_at'] = datetime.now().isoformat()
    
    print(f"Transformed courses: {len(df)} rows")
    
    output_path = PROCESSED_DIR / 'courses_transformed.csv'
    df.to_csv(output_path, index=False)
    
    ti.xcom_push(key='courses_transformed_path', value=str(output_path))
    return str(output_path)


def transform_records(**context):
    """Transform and clean records data"""
    ti = context['task_instance']
    records_path = ti.xcom_pull(task_ids='ingest_records', key='records_path')
    
    df = pd.read_csv(records_path)
    
    # Handle missing values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df[col].fillna(df[col].median(), inplace=True)
    
    # Standardize string columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip().str.upper()
        df[col].fillna('UNKNOWN', inplace=True)
    
    # Add processing timestamp
    df['processed_at'] = datetime.now().isoformat()
    
    print(f"Transformed records: {len(df)} rows")
    
    output_path = PROCESSED_DIR / 'records_transformed.csv'
    df.to_csv(output_path, index=False)
    
    ti.xcom_push(key='records_transformed_path', value=str(output_path))
    return str(output_path)


def merge_datasets(**context):
    """Merge courses and records on course_id"""
    ti = context['task_instance']
    
    courses_path = ti.xcom_pull(task_ids='transformation_group.transform_courses',
                                 key='courses_transformed_path')
    records_path = ti.xcom_pull(task_ids='transformation_group.transform_records',
                                 key='records_transformed_path')
    
    courses_df = pd.read_csv(courses_path)
    records_df = pd.read_csv(records_path)
    
    print(f"Courses shape: {courses_df.shape}")
    print(f"Records shape: {records_df.shape}")
    
    # Find merge key (course_id or similar)
    common_columns = set(courses_df.columns) & set(records_df.columns)
    potential_keys = ['COURSE_ID', 'ID', 'COURSEID']
    
    merge_key = None
    for key in potential_keys:
        if key in common_columns:
            merge_key = key
            break
    
    if merge_key:
        # Merge on course_id (LEFT JOIN to keep all records)
        merged_df = pd.merge(records_df, courses_df, on=merge_key, how='left',
                           suffixes=('_record', '_course'))
        print(f"Merged on: {merge_key}")
    else:
        print("Warning: No common course_id found, creating demo merge")
        # Fallback: take first ID-like column from each
        records_id = [col for col in records_df.columns if 'ID' in col.upper()][0]
        courses_id = [col for col in courses_df.columns if 'ID' in col.upper()][0]
        merged_df = pd.merge(records_df, courses_df, left_on=records_id, 
                           right_on=courses_id, how='left', suffixes=('_record', '_course'))
    
    print(f"Merged dataset: {merged_df.shape}")
    
    output_path = PROCESSED_DIR / 'merged_data.csv'
    merged_df.to_csv(output_path, index=False)
    
    ti.xcom_push(key='merged_path', value=str(output_path))
    return str(output_path)


def load_to_postgres(**context):
    """Load merged data into PostgreSQL"""
    ti = context['task_instance']
    merged_path = ti.xcom_pull(task_ids='merge_datasets', key='merged_path')
    
    df = pd.read_csv(merged_path)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    table_name = 'super_mario_maker_data'
    
    # Bulk insert to PostgreSQL
    df.to_sql(
        table_name,
        postgres_hook.get_sqlalchemy_engine(),
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Loaded {len(df)} rows to table: {table_name}")
    ti.xcom_push(key='table_name', value=table_name)
    return table_name


def analyze_data(**context):
    """Perform gaming-specific analysis"""
    ti = context['task_instance']
    table_name = ti.xcom_pull(task_ids='load_to_postgres', key='table_name')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Read from database
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, postgres_hook.get_sqlalchemy_engine())
    
    print(f"Retrieved {len(df)} rows for analysis")
    
    # Analysis results
    analysis_results = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': list(df.columns),
        'numeric_stats': {},
        'gaming_insights': {}
    }
    
    # Basic statistics
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        analysis_results['numeric_stats'][col] = {
            'mean': float(df[col].mean()),
            'median': float(df[col].median()),
            'std': float(df[col].std()),
            'min': float(df[col].min()),
            'max': float(df[col].max())
        }
    
    # Gaming-specific analysis
    print("\n=== Gaming Data Analysis ===")
    
    # Difficulty distribution
    diff_cols = [col for col in df.columns if 'DIFFICULTY' in col.upper()]
    if diff_cols:
        diff_col = diff_cols[0]
        diff_dist = df[diff_col].value_counts().to_dict()
        analysis_results['gaming_insights']['difficulty_distribution'] = diff_dist
        print(f"Difficulty Distribution: {diff_dist}")
    
    # Clear rates
    clear_cols = [col for col in df.columns if 'CLEAR' in col.upper() and 'RATE' in col.upper()]
    if clear_cols:
        clear_col = clear_cols[0]
        analysis_results['gaming_insights']['average_clear_rate'] = float(df[clear_col].mean())
        print(f"Average Clear Rate: {df[clear_col].mean():.2f}%")
    
    # Completion times
    time_cols = [col for col in df.columns if 'TIME' in col.upper() and 'CLEAR' in col.upper()]
    if time_cols:
        time_col = time_cols[0]
        analysis_results['gaming_insights']['fastest_time'] = float(df[time_col].min())
        analysis_results['gaming_insights']['average_time'] = float(df[time_col].mean())
        print(f"Times - Min: {df[time_col].min():.2f}s, Avg: {df[time_col].mean():.2f}s")
    
    # Play counts
    play_cols = [col for col in df.columns if 'PLAY' in col.upper() and 'COUNT' in col.upper()]
    if play_cols:
        play_col = play_cols[0]
        analysis_results['gaming_insights']['total_plays'] = int(df[play_col].sum())
        print(f"Total Plays: {df[play_col].sum():,}")
    
    # Save analysis
    analysis_path = PROCESSED_DIR / 'analysis_results.json'
    with open(analysis_path, 'w') as f:
        json.dump(analysis_results, f, indent=2)
    
    print(f"\nAnalysis saved to {analysis_path}")
    
    # Create visualizations
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Super Mario Maker Data Analysis', fontsize=16, fontweight='bold')
        
        # Plot 1: Numeric distribution
        if len(numeric_cols) > 0:
            axes[0, 0].hist(df[numeric_cols[0]].dropna(), bins=30, color='steelblue', edgecolor='black')
            axes[0, 0].set_title(f'{numeric_cols[0]} Distribution')
            axes[0, 0].grid(alpha=0.3)
        
        # Plot 2: Difficulty distribution
        if diff_cols:
            diff_data = df[diff_cols[0]].value_counts()
            axes[0, 1].bar(range(len(diff_data)), diff_data.values, color='coral', edgecolor='black')
            axes[0, 1].set_xticks(range(len(diff_data)))
            axes[0, 1].set_xticklabels(diff_data.index, rotation=45)
            axes[0, 1].set_title('Difficulty Distribution')
            axes[0, 1].grid(axis='y', alpha=0.3)
        
        # Plot 3: Clear rates
        if clear_cols:
            axes[1, 0].hist(df[clear_cols[0]].dropna(), bins=20, color='lightgreen', edgecolor='black')
            axes[1, 0].set_title('Clear Rate Distribution')
            axes[1, 0].grid(alpha=0.3)
        
        # Plot 4: Top courses
        if play_cols and len(df) > 0:
            top_data = df.nlargest(min(10, len(df)), play_cols[0])
            axes[1, 1].barh(range(len(top_data)), top_data[play_cols[0]], color='skyblue', edgecolor='black')
            axes[1, 1].set_title('Top Played Courses')
            axes[1, 1].invert_yaxis()
            axes[1, 1].grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        
        viz_path = PROCESSED_DIR / 'gaming_analysis.png'
        plt.savefig(viz_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"Visualization saved to {viz_path}")
        
    except ImportError:
        print("Matplotlib not available, skipping visualization")
    except Exception as e:
        print(f"Error creating visualization: {str(e)}")
    
    return str(analysis_path)


def cleanup(**context):
    """Clean up intermediate files"""
    intermediate_files = [
        'courses_ingested.csv',
        'records_ingested.csv',
        'courses_transformed.csv',
        'records_transformed.csv'
    ]
    
    deleted_count = 0
    for filename in intermediate_files:
        file_path = PROCESSED_DIR / filename
        if file_path.exists():
            file_path.unlink()
            deleted_count += 1
            print(f"Deleted: {file_path}")
    
    print(f"Cleanup complete. Deleted {deleted_count} files")
    return deleted_count


# Define tasks
task_setup = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag,
)

task_create_table = PostgresOperator(
    task_id='create_postgres_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS super_mario_maker_data (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# Parallel ingestion
task_ingest_courses = PythonOperator(
    task_id='ingest_courses',
    python_callable=ingest_courses,
    dag=dag,
)

task_ingest_records = PythonOperator(
    task_id='ingest_records',
    python_callable=ingest_records,
    dag=dag,
)

# Parallel transformation (using TaskGroup)
with TaskGroup('transformation_group', dag=dag) as transformation_group:
    task_transform_courses = PythonOperator(
        task_id='transform_courses',
        python_callable=transform_courses,
    )
    
    task_transform_records = PythonOperator(
        task_id='transform_records',
        python_callable=transform_records,
    )

task_merge = PythonOperator(
    task_id='merge_datasets',
    python_callable=merge_datasets,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

task_analyze = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

task_cleanup = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup,
    dag=dag,
)

# Define dependencies (parallel processing)
task_setup >> task_create_table
task_create_table >> [task_ingest_courses, task_ingest_records]

# Connect to transformations
task_ingest_courses >> transformation_group
task_ingest_records >> transformation_group

# Sequential after merge
transformation_group >> task_merge >> task_load >> task_analyze >> task_cleanup