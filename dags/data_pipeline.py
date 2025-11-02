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
        
        # Read with low_memory=False to handle mixed types
        df = pd.read_csv(input_path, low_memory=False)
        print(f"Ingested courses: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        print(f"Sample data:\n{df.head(3)}")
        
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
        
        # Read with low_memory=False to handle mixed types
        df = pd.read_csv(input_path, low_memory=False)
        print(f"Ingested records: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        print(f"Sample data:\n{df.head(3)}")
        
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
    
    df = pd.read_csv(courses_path, low_memory=False)
    print(f"Original courses shape: {df.shape}")
    print(f"Original columns: {list(df.columns)}")
    
    # Handle missing values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isna().sum() > 0:
            df[col].fillna(df[col].median(), inplace=True)
            print(f"Filled {df[col].isna().sum()} missing values in {col}")
    
    # Standardize string columns (handle with care - don't uppercase IDs)
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        # Don't uppercase ID columns
        if 'id' not in col.lower():
            df[col] = df[col].astype(str).str.strip()
        df[col].fillna('UNKNOWN', inplace=True)
    
    # Remove any duplicate rows
    before_dedup = len(df)
    df = df.drop_duplicates()
    after_dedup = len(df)
    if before_dedup != after_dedup:
        print(f"Removed {before_dedup - after_dedup} duplicate rows")
    
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
    
    df = pd.read_csv(records_path, low_memory=False)
    print(f"Original records shape: {df.shape}")
    print(f"Original columns: {list(df.columns)}")
    
    # Handle missing values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isna().sum() > 0:
            df[col].fillna(df[col].median(), inplace=True)
            print(f"Filled {df[col].isna().sum()} missing values in {col}")
    
    # Standardize string columns (handle with care - don't uppercase IDs)
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        # Don't uppercase ID columns
        if 'id' not in col.lower():
            df[col] = df[col].astype(str).str.strip()
        df[col].fillna('UNKNOWN', inplace=True)
    
    # Remove any duplicate rows
    before_dedup = len(df)
    df = df.drop_duplicates()
    after_dedup = len(df)
    if before_dedup != after_dedup:
        print(f"Removed {before_dedup - after_dedup} duplicate rows")
    
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
    
    courses_df = pd.read_csv(courses_path, low_memory=False)
    records_df = pd.read_csv(records_path, low_memory=False)
    
    print(f"Courses shape: {courses_df.shape}")
    print(f"Records shape: {records_df.shape}")
    print(f"Courses columns: {list(courses_df.columns)}")
    print(f"Records columns: {list(records_df.columns)}")
    
    # Find the merge key - look for common column containing 'course' or 'id'
    common_columns = set(courses_df.columns) & set(records_df.columns)
    print(f"Common columns: {common_columns}")
    
    # Prioritize looking for course_id or similar
    potential_keys = [col for col in common_columns if 'course' in col.lower() and 'id' in col.lower()]
    
    if not potential_keys:
        # Fallback: look for any column with 'id' that appears in both
        potential_keys = [col for col in common_columns if 'id' in col.lower()]
    
    if potential_keys:
        merge_key = potential_keys[0]
        print(f"Merging on: {merge_key}")
        
        # LEFT JOIN to keep all records and match with course data
        merged_df = pd.merge(records_df, courses_df, on=merge_key, how='left',
                           suffixes=('_record', '_course'))
        
        print(f"Successfully merged on {merge_key}")
        print(f"Merged dataset: {merged_df.shape}")
        print(f"Null values after merge: {merged_df.isnull().sum().sum()}")
        
    else:
        raise ValueError("No suitable merge key found between datasets. "
                        "Please ensure both files have a common 'course_id' or similar column.")
    
    output_path = PROCESSED_DIR / 'merged_data.csv'
    merged_df.to_csv(output_path, index=False)
    
    ti.xcom_push(key='merged_path', value=str(output_path))
    ti.xcom_push(key='merge_key', value=merge_key)
    return str(output_path)


def load_to_postgres(**context):
    """Load merged data into PostgreSQL with optimized schema"""
    ti = context['task_instance']
    merged_path = ti.xcom_pull(task_ids='merge_datasets', key='merged_path')
    
    df = pd.read_csv(merged_path, low_memory=False)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    table_name = 'super_mario_maker_data'
    
    # Bulk insert to PostgreSQL with progress tracking
    print(f"Loading {len(df)} rows to PostgreSQL...")
    
    df.to_sql(
        table_name,
        postgres_hook.get_sqlalchemy_engine(),
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Successfully loaded {len(df)} rows to table: {table_name}")
    
    # Create analytics views
    engine = postgres_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        # Create index for better query performance
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_course_id ON {table_name} (course_id)")
        conn.commit()
    
    ti.xcom_push(key='table_name', value=table_name)
    return table_name


def analyze_data(**context):
    """Perform comprehensive gaming-specific analysis"""
    ti = context['task_instance']
    table_name = ti.xcom_pull(task_ids='load_to_postgres', key='table_name')
    merge_key = ti.xcom_pull(task_ids='merge_datasets', key='merge_key')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Read from database
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, postgres_hook.get_sqlalchemy_engine())
    
    print(f"Retrieved {len(df)} rows for analysis")
    print(f"Available columns: {list(df.columns)}")
    
    # Analysis results
    analysis_results = {
        'timestamp': datetime.now().isoformat(),
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': list(df.columns),
        'merge_key': merge_key,
        'numeric_stats': {},
        'gaming_insights': {},
        'data_quality': {}
    }
    
    # Data quality metrics
    analysis_results['data_quality']['missing_values'] = df.isnull().sum().to_dict()
    analysis_results['data_quality']['duplicate_rows'] = int(df.duplicated().sum())
    
    # Basic statistics for numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        analysis_results['numeric_stats'][col] = {
            'mean': float(df[col].mean()),
            'median': float(df[col].median()),
            'std': float(df[col].std()),
            'min': float(df[col].min()),
            'max': float(df[col].max()),
            'q25': float(df[col].quantile(0.25)),
            'q75': float(df[col].quantile(0.75))
        }
    
    # Gaming-specific analysis
    print("\n=== Gaming Data Analysis ===")
    
    # Analyze difficulty distribution
    diff_cols = [col for col in df.columns if 'difficult' in col.lower()]
    if diff_cols:
        diff_col = diff_cols[0]
        diff_dist = df[diff_col].value_counts().to_dict()
        analysis_results['gaming_insights']['difficulty_distribution'] = diff_dist
        print(f"Difficulty Distribution ({diff_col}): {diff_dist}")
    
    # Analyze clear rates
    clear_cols = [col for col in df.columns if 'clear' in col.lower() and ('rate' in col.lower() or 'count' in col.lower())]
    if clear_cols:
        for clear_col in clear_cols:
            if 'rate' in clear_col.lower():
                analysis_results['gaming_insights'][f'{clear_col}_stats'] = {
                    'average': float(df[clear_col].mean()),
                    'median': float(df[clear_col].median()),
                    'min': float(df[clear_col].min()),
                    'max': float(df[clear_col].max())
                }
                print(f"{clear_col} - Mean: {df[clear_col].mean():.2f}, Median: {df[clear_col].median():.2f}")
    
    # Analyze completion times
    time_cols = [col for col in df.columns if 'time' in col.lower() and 'clear' in col.lower()]
    if time_cols:
        time_col = time_cols[0]
        analysis_results['gaming_insights'][f'{time_col}_stats'] = {
            'fastest': float(df[time_col].min()),
            'slowest': float(df[time_col].max()),
            'average': float(df[time_col].mean()),
            'median': float(df[time_col].median())
        }
        print(f"{time_col} - Min: {df[time_col].min():.2f}s, Max: {df[time_col].max():.2f}s, Avg: {df[time_col].mean():.2f}s")
    
    # Analyze play counts and engagement
    play_cols = [col for col in df.columns if 'play' in col.lower() and 'count' in col.lower()]
    like_cols = [col for col in df.columns if 'like' in col.lower() and 'count' in col.lower()]
    
    if play_cols:
        play_col = play_cols[0]
        analysis_results['gaming_insights']['engagement'] = {
            'total_plays': int(df[play_col].sum()),
            'average_plays_per_course': float(df[play_col].mean()),
            'most_played': int(df[play_col].max())
        }
        print(f"Total Plays: {df[play_col].sum():,}")
        
        # Identify popular courses
        if len(df) > 0:
            top_courses = df.nlargest(min(5, len(df)), play_col)
            popular = []
            for idx, row in top_courses.iterrows():
                popular.append({
                    'plays': int(row[play_col]),
                    'course_id': str(row[merge_key]) if merge_key in row else 'Unknown'
                })
            analysis_results['gaming_insights']['top_5_courses'] = popular
            print(f"Top 5 Most Played Courses: {popular}")
    
    if like_cols:
        like_col = like_cols[0]
        analysis_results['gaming_insights']['likes'] = {
            'total_likes': int(df[like_col].sum()),
            'average_likes': float(df[like_col].mean())
        }
        print(f"Total Likes: {df[like_col].sum():,}")
    
    # Calculate engagement rate if both plays and likes exist
    if play_cols and like_cols:
        engagement_rate = (df[like_cols[0]].sum() / df[play_cols[0]].sum()) * 100
        analysis_results['gaming_insights']['engagement_rate'] = float(engagement_rate)
        print(f"Overall Engagement Rate (Likes/Plays): {engagement_rate:.2f}%")
    
    # Analyze attempts (if available)
    attempt_cols = [col for col in df.columns if 'attempt' in col.lower()]
    if attempt_cols:
        attempt_col = attempt_cols[0]
        analysis_results['gaming_insights'][f'{attempt_col}_stats'] = {
            'average': float(df[attempt_col].mean()),
            'median': float(df[attempt_col].median()),
            'max': int(df[attempt_col].max())
        }
        print(f"Attempts - Avg: {df[attempt_col].mean():.2f}, Median: {df[attempt_col].median():.2f}")
    
    # Save analysis results
    analysis_path = PROCESSED_DIR / 'analysis_results.json'
    with open(analysis_path, 'w') as f:
        json.dump(analysis_results, f, indent=2)
    
    print(f"\n✅ Analysis saved to {analysis_path}")
    
    # Create visualizations
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        sns.set_style("whitegrid")
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Super Mario Maker Gaming Data Analysis', fontsize=18, fontweight='bold', y=0.995)
        
        # Plot 1: Difficulty Distribution
        if diff_cols:
            ax = axes[0, 0]
            diff_data = df[diff_cols[0]].value_counts().sort_index()
            colors = sns.color_palette("husl", len(diff_data))
            bars = ax.bar(range(len(diff_data)), diff_data.values, color=colors, edgecolor='black', linewidth=1.5)
            ax.set_xticks(range(len(diff_data)))
            ax.set_xticklabels(diff_data.index, rotation=15, ha='right')
            ax.set_title('Course Difficulty Distribution', fontsize=14, fontweight='bold')
            ax.set_ylabel('Number of Courses', fontsize=11)
            ax.grid(axis='y', alpha=0.3)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=10, fontweight='bold')
        else:
            axes[0, 0].text(0.5, 0.5, 'Difficulty data not available',
                           ha='center', va='center', fontsize=12)
            axes[0, 0].axis('off')
        
        # Plot 2: Clear Rate Distribution
        if clear_cols:
            clear_rate_col = [col for col in clear_cols if 'rate' in col.lower()]
            if clear_rate_col:
                ax = axes[0, 1]
                clear_data = df[clear_rate_col[0]].dropna()
                ax.hist(clear_data, bins=30, color='lightgreen', edgecolor='black', linewidth=1.2)
                ax.axvline(clear_data.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {clear_data.mean():.2f}%')
                ax.axvline(clear_data.median(), color='blue', linestyle='--', linewidth=2, label=f'Median: {clear_data.median():.2f}%')
                ax.set_title('Clear Rate Distribution', fontsize=14, fontweight='bold')
                ax.set_xlabel('Clear Rate (%)', fontsize=11)
                ax.set_ylabel('Frequency', fontsize=11)
                ax.legend(fontsize=10)
                ax.grid(alpha=0.3)
            else:
                axes[0, 1].text(0.5, 0.5, 'Clear rate data not available',
                               ha='center', va='center', fontsize=12)
                axes[0, 1].axis('off')
        else:
            axes[0, 1].text(0.5, 0.5, 'Clear rate data not available',
                           ha='center', va='center', fontsize=12)
            axes[0, 1].axis('off')
        
        # Plot 3: Completion Time Distribution
        if time_cols:
            ax = axes[1, 0]
            time_data = df[time_cols[0]].dropna()
            ax.hist(time_data, bins=30, color='skyblue', edgecolor='black', linewidth=1.2)
            ax.axvline(time_data.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {time_data.mean():.2f}s')
            ax.set_title('Completion Time Distribution', fontsize=14, fontweight='bold')
            ax.set_xlabel('Time (seconds)', fontsize=11)
            ax.set_ylabel('Frequency', fontsize=11)
            ax.legend(fontsize=10)
            ax.grid(alpha=0.3)
        else:
            axes[1, 0].text(0.5, 0.5, 'Completion time data not available',
                           ha='center', va='center', fontsize=12)
            axes[1, 0].axis('off')
        
        # Plot 4: Top Courses by Plays
        if play_cols and len(df) > 0:
            ax = axes[1, 1]
            top_n = min(10, len(df))
            top_courses = df.nlargest(top_n, play_cols[0])
            
            # Create labels (truncate if too long)
            labels = []
            for idx, row in top_courses.iterrows():
                label = str(row[merge_key]) if merge_key in row else f"Course {idx}"
                if len(label) > 20:
                    label = label[:17] + '...'
                labels.append(label)
            
            colors = sns.color_palette("viridis", top_n)
            bars = ax.barh(range(top_n), top_courses[play_cols[0]], color=colors, edgecolor='black', linewidth=1.2)
            ax.set_yticks(range(top_n))
            ax.set_yticklabels(labels, fontsize=9)
            ax.set_title(f'Top {top_n} Most Played Courses', fontsize=14, fontweight='bold')
            ax.set_xlabel('Number of Plays', fontsize=11)
            ax.invert_yaxis()
            ax.grid(axis='x', alpha=0.3)
            
            # Add value labels on bars
            for i, (bar, val) in enumerate(zip(bars, top_courses[play_cols[0]])):
                ax.text(val, bar.get_y() + bar.get_height()/2.,
                       f' {int(val):,}',
                       va='center', fontsize=9, fontweight='bold')
        else:
            axes[1, 1].text(0.5, 0.5, 'Play count data not available',
                           ha='center', va='center', fontsize=12)
            axes[1, 1].axis('off')
        
        plt.tight_layout()
        
        viz_path = PROCESSED_DIR / 'gaming_analysis.png'
        plt.savefig(viz_path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"📊 Visualization saved to {viz_path}")
        
    except ImportError as ie:
        print(f"Visualization libraries not available: {str(ie)}")
    except Exception as e:
        print(f"Error creating visualization: {str(e)}")
        import traceback
        traceback.print_exc()
    
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
            print(f"🗑️  Deleted: {file_path}")
    
    print(f"✅ Cleanup complete. Deleted {deleted_count} intermediate files")
    print(f"📁 Final outputs preserved in: {PROCESSED_DIR}")
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

# Parallel transformation (using TaskGroup for better organization)
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

# Connect to transformations (parallel)
task_ingest_courses >> transformation_group
task_ingest_records >> transformation_group

# Sequential after merge
transformation_group >> task_merge >> task_load >> task_analyze >> task_cleanup