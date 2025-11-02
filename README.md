# IDS706-Week10-Major-Assignment

## 🎮 Super Mario Maker Data Pipeline with Apache Airflow

### Project Overview
An end-to-end data orchestration pipeline using Apache Airflow that processes Super Mario Maker gaming data (courses and player records) with parallel processing, PostgreSQL storage, and gaming-specific analytics.

**Dataset**: [Super Mario Maker (SMMnet)](https://www.kaggle.com/datasets/leomauro/smmnet) - 880K+ courses, 7M+ player interactions

### 📁 Project Structure

```
IDS706-Week10-Major-Assignment/
├── .devcontainer/              # Dev container configuration
├── dags/
│   └── data_pipeline.py        # Main Airflow DAG (Super Mario Maker pipeline)
├── data/                       # 👈 PUT YOUR DATA HERE
│   ├── courses.csv             # (Super Mario Maker courses - your input)
│   ├── records.csv             # (Player records - your input)
│   ├── processed/              # (Pipeline outputs)
│   └── scripts/
│       └── generate_data.py    # Generate sample gaming data
├── screenshots/                # DAG execution screenshots
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # Custom Airflow image
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

### 🚀 Quick Start

#### Option 1: Use Sample Data (Recommended First)
```bash
# Generate realistic Super Mario Maker test data
python data/scripts/generate_data.py

# Start Airflow
docker-compose up -d

# Access UI: http://localhost:8080 (admin/admin)
# Enable and trigger: data_pipeline
```

#### Option 2: Use Real Kaggle Data
```bash
# Download from Kaggle
kaggle datasets download -d leomauro/smmnet
unzip smmnet.zip
cp courses.csv data/
cp records.csv data/

# Start Airflow
docker-compose up -d
```

### 📊 Pipeline Description

**DAG Name:** `data_pipeline` (Super Mario Maker Pipeline)

**Schedule:** Daily (`@daily`)

**Tasks (with parallel processing):**
1. `setup_directories` - Initialize folder structure
2. `create_postgres_table` - Setup database
3. `ingest_courses` & `ingest_records` - Parallel data ingestion ⚡
4. `transform_courses` & `transform_records` - Parallel transformations ⚡
5. `merge_datasets` - Merge on course_id
6. `load_to_postgres` - Bulk load to database
7. `analyze_data` - Gaming-specific analysis:
   - Difficulty distribution
   - Clear rate trends
   - Player performance metrics
   - Popular courses identification
8. `cleanup` - Remove intermediate files

**Key Features:**
- ✅ Parallel task execution (ingestion & transformation)
- ✅ XCom for file paths only (not data)
- ✅ PostgreSQL with gaming-optimized schema
- ✅ Gaming-specific analysis & 4-panel visualizations
- ✅ Automated cleanup

### 📂 Data Location

```
data/
├── courses.csv          👈 PUT THIS HERE (Super Mario Maker courses)
├── records.csv          👈 PUT THIS HERE (Player completion records)
└── processed/           👈 RESULTS APPEAR HERE
    ├── merged_data.csv
    ├── analysis_results.json
    └── gaming_analysis.png
```

### 🎮 Datasets Used

**Dataset 1: courses.csv** - Super Mario Maker game courses/levels
- Columns: `course_id`, `course_name`, `difficulty`, `clear_rate`, `play_count`, `like_count`, `game_style`, `theme`, etc.
- Source: [Kaggle SMMnet](https://www.kaggle.com/datasets/leomauro/smmnet)

**Dataset 2: records.csv** - Player completion records
- Columns: `record_id`, `course_id`, `player_id`, `clear_time`, `attempts`, `recorded_at`, etc.
- Links to courses via `course_id`

**Merge Key:** `course_id` (links player records to course details)

### 📈 Analysis Results

The pipeline generates comprehensive gaming analytics:

**Difficulty Distribution:**
- Breakdown by Easy/Normal/Expert/Super Expert
- Visual bar chart

**Clear Rate Analysis:**
- Average: ~18% (typical for SMM)
- Median and distribution
- Correlation with difficulty

**Player Performance:**
- Fastest completion times
- Average times by difficulty
- Attempt counts

**Popular Courses:**
- Top 5 most played
- Engagement metrics
- Play/like ratios

**Outputs:**
- JSON file with detailed insights
- 4-panel visualization PNG
- PostgreSQL tables with analytics views

### 📸 Screenshots

![DAG Graph View](screenshots/dag_graph_view.png)
*Parallel processing of courses and records data*

![Successful Execution](screenshots/successful_run.png)
*All tasks completed successfully*

### 🗄️ Database Schema

**Main Table:** `super_mario_maker_data`

**Views:**
- `course_statistics` - Per-course metrics
- `difficulty_distribution` - Difficulty-based analytics
- `player_performance` - Player rankings
- `popular_courses` - Top 100 by engagement

Access with:
```bash
docker exec -it postgres psql -U airflow -d airflow
\dt  # List tables
SELECT * FROM course_statistics LIMIT 5;
```

### 🛠️ Technologies Used
- **Apache Airflow 3.1.1** - Workflow orchestration
- **PostgreSQL 15** - Data warehouse
- **Python 3.13** - Data processing
- **Docker** - Containerization
- **Pandas** - Data manipulation
- **Matplotlib/Seaborn** - Visualizations
- **SQLAlchemy** - Database ORM

### 🧪 Testing

```bash
# Validate DAG syntax
python dags/data_pipeline.py

# Check data files
ls data/*.csv

# View logs
docker-compose logs -f airflow-scheduler
```

### 📋 Setup Instructions

#### Prerequisites
- Docker and Docker Compose
- Git
- Python 3.8+ (for data generation)

#### Installation

1. **Clone the repository:**
```bash
git clone <your-repo-url>
cd IDS706-Week10-Major-Assignment
```

2. **Add your datasets** (choose one):
```bash
# Option A: Generate sample data
python data/scripts/generate_data.py

# Option B: Use your own data
cp /path/to/courses.csv data/
cp /path/to/records.csv data/
```

3. **Start services:**
```bash
docker-compose up -d
```

4. **Access Airflow UI:**
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

5. **Configure PostgreSQL connection** (in Airflow UI):
- Connection ID: `postgres_default`
- Host: `postgres`
- Schema: `airflow`
- Login: `airflow`
- Password: `airflow`
- Port: `5432`

6. **Run the pipeline:**
- Enable the `data_pipeline` DAG
- Click "Trigger DAG"
- Monitor execution in Graph view

### 🔧 Troubleshooting

**DAG not showing?**
```bash
docker-compose restart airflow-scheduler
docker-compose logs airflow-scheduler
```

**Data not found?**
```bash
ls data/*.csv  # Verify files exist
python data/scripts/generate_data.py  # Generate sample data
```

**Connection error?**
- Verify PostgreSQL connection in Airflow UI
- Check `docker-compose ps` - all services should be running

### ✅ Assignment Requirements Met

- [x] Deploy Airflow with Docker
- [x] Create DAG with schedule (`@daily`)
- [x] Two related datasets (courses + records)
- [x] Data transformations (cleaning, standardization)
- [x] TaskGroups for organization
- [x] Merge datasets on common key (`course_id`)
- [x] Load to PostgreSQL with bulk insert
- [x] Analysis with visualizations
- [x] Cleanup intermediate files
- [x] XCom best practices (paths only)
- [x] Parallel processing (ingestion + transformation)
- [x] Containerized development setup
- [x] Documentation

### 🎓 What This Demonstrates

- **Data Engineering**: ETL pipeline design with Airflow
- **Parallel Processing**: Concurrent task execution
- **Database Integration**: PostgreSQL with analytics views
- **Domain Knowledge**: Gaming industry data analysis
- **Software Engineering**: Production-ready code patterns
- **DevOps**: Docker containerization

### 📚 Additional Resources

- **Airflow Documentation**: https://airflow.apache.org/docs/
- **Dataset Source**: https://www.kaggle.com/datasets/leomauro/smmnet
- **PostgreSQL Docs**: https://www.postgresql.org/docs/

### 👤 Author

**Your Name**
- Course: IDS706 - Data Engineering
- Assignment: Week 10 Major Assignment
- Date: November 2025

---

**Ready to run!** Generate data, start Docker, trigger the DAG. 🚀🎮