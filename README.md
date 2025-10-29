# IDS706-Week10-Major-Assignment

## Airflow Data Pipeline with PostgreSQL

### Project Overview
This project implements an end-to-end data orchestration pipeline using Apache Airflow. The pipeline ingests two related datasets, performs transformations, merges the data, stores it in PostgreSQL, and conducts analysis.

### Features
- ✅ Parallel task execution
- ✅ XCom for inter-task communication (file paths only)
- ✅ PostgreSQL database integration
- ✅ Containerized development environment
- ✅ Automated data cleanup

### Project Structure
```
├── .devcontainer/          # Dev container configuration
├── dags/                   # Airflow DAG files
├── data/                   # Raw datasets
├── screenshots/            # DAG execution screenshots
├── docker-compose.yml      # Docker orchestration
├── Dockerfile              # Custom Airflow image
└── requirements.txt        # Python dependencies
```

### Setup Instructions

#### Prerequisites
- Docker and Docker Compose
- Git

#### Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd IDS706-Week10-Major-Assignment
```

2. Add your datasets to the `data/` folder

3. Start the services:
```bash
docker-compose up -d
```

4. Access Airflow UI:
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### Pipeline Description

**DAG Name:** `data_pipeline`

**Schedule:** Daily (`@daily`)

**Tasks:**
1. `ingest_dataset1` - Ingest first dataset (parallel)
2. `ingest_dataset2` - Ingest second dataset (parallel)
3. `transform_dataset1` - Clean and transform dataset 1
4. `transform_dataset2` - Clean and transform dataset 2
5. `merge_datasets` - Merge both datasets
6. `load_to_postgres` - Load to PostgreSQL database
7. `analyze_data` - Perform analysis/visualization
8. `cleanup` - Remove intermediate files

### Datasets Used
- Dataset 1: [Description]
- Dataset 2: [Description]
- Source: [URL]

### Analysis Results
[Describe your analysis findings]

### Screenshots
![DAG Graph View](screenshots/dag_graph_view.png)
![Successful Execution](screenshots/successful_run.png)

### Technologies Used
- Apache Airflow 3.1.1
- PostgreSQL 15
- Python 3.13
- Docker
- Pandas, SQLAlchemy, Scikit-learn

### Author
[Your Name]