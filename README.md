# Catalonia Weather Data Pipeline (XEMA)

This is an end-to-end real-world batch data data pipeline built on Google Cloud to ingest, process, and visualize real-time meteorological data from Catalonia (XEMA).

## Architecture Components
- **Data Source**: XEMA Meteorological Data (Open Data Catalonia)
- **Infrastructure as Code**: Terraform
- **Ingestion code**: Python (requests, pandas)
- **Data Lake Storage**: Google Cloud Storage (GCS)
- **Data Warehouse**: Google BigQuery
- **Orchestration Engine**: Apache Airflow
- **Data Transformation**: dbt (data build tool)
- **Visualization BI**: Looker Studio

## Repository Structure
```text
catalonia-weather-pipeline/
├── terraform/                # GCP Infrastructure (GCS, BigQuery, IAM)
├── dags/                     # Airflow DAG for orchestration
│   └── scripts/              # Python ingestion script fetching API data
├── dbt_xema/                 # dbt transformations (staging and core data models)
├── bigquery/                 # BigQuery external JSON table schemas
└── README.md                 # Project documentation
```

## Setup Instructions

### 1. Environment & GitHub Codespaces Support
This repository includes a `.devcontainer` and supports **GitHub Codespaces** for an instant development environment.
- **Run in Codespaces**: Click "Code" -> "Codespaces" -> "New codespace" on GitHub. The IDE will boot with Python 3.11, Terraform, and extensions pre-configured.
- **Run Locally**: Ensure you have Python 3.11 installed, clone the repository, and run `pip install -r dags/scripts/requirements.txt`.

### 2. Configure Environment Variables & Secrets
We use `python-dotenv` to manage secrets securely, meaning no hardcoded credentials inside scripts.
1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Set your `GCP_PROJECT_ID` and `GCP_BUCKET_NAME`.
3. Provide authentication:
   - **Locally**: Provide the path to a Google Cloud Service Account JSON key in `GOOGLE_APPLICATION_CREDENTIALS`
   - **GitHub Codespaces**: Create a GitHub Codespaces Repository Secret named `GCP_SA_KEY` containing the entire JSON content of your Service Account Key. The Codespaces setup script will automatically reconstruct this key in `/tmp/gcp-key.json` and configure `GOOGLE_APPLICATION_CREDENTIALS` for you.
   
*Note: Never commit your `.env` or Service Account `credentials/` folder. They are excluded via `.gitignore`.*

### 3. Setup Google Cloud & Terraform
1. Ensure your `.env` file or environment variables are sourced.
2. Navigate to the `terraform/` directory.
3. Initialize Terraform plugins:
   ```bash
   terraform init
   ```
4. Review the execution plan and apply the infrastructure. Pass the necessary variables dynamically from your environment:
   ```bash
   terraform apply -var="project_id=$GCP_PROJECT_ID" -var="bucket_name=$GCP_BUCKET_NAME"
   ```

### 4. Run Pipeline (Airflow Data Ingestion)
1. Run Apache Airflow locally or in your Codespace.
2. The `xema_daily_weather_pipeline` DAG securely reads environment variables `GCP_PROJECT_ID` and `GCP_BUCKET_NAME`.
3. Ensure `.env` is loaded by Airflow natively, or export the variables in the terminal before running the Airflow scheduler.
4. Manually trigger a DAG run through the Airflow Web UI to execute the extraction, load the data to Google Cloud Storage Data Lake, and insert it into BigQuery staging.

### 4. Data Transformations (dbt)
The Airflow DAG executes dbt transformations automatically. However, you can run them manually to test the models:
1. Navigate to the `dbt_xema/` directory.
2. Make sure you copy `profiles.yml.example` to `~/.dbt/profiles.yml` or place it in the same directory and update your BigQuery project settings and service account keys.
3. Validate connection:
   ```bash
   dbt debug --profiles-dir .
   ```
4. Run transformations and tests to build `fct_daily_weather`:
   ```bash
   dbt build --profiles-dir .
   ```

### 5. View Dashboard (Looker Studio Guidelines)
Once your BigQuery dataset is populated with the `fct_daily_weather` table, you can connect it easily to Google Looker Studio.

**Looker Studio Configuration:**
1. Open Looker Studio and create a New Report.
2. Add Data -> select BigQuery -> choose your Project, Dataset (`xema_weather`), and Table (`fct_daily_weather`).

**Suggested Dashboard Tiles:**
1. **Time Series Line Chart (Weather trends)**
   - *Dimension*: `reading_date`
   - *Breakdown Dimension*: `station_code`
   - *Metric*: `avg_daily_value` (Aggregation: Average)
   - *Insight*: Shows weather value fluctuations across different Catalonia stations over the last week.
   
2. **Categorical Distribution Bar Chart (Maximums by Station)**
   - *Dimension*: `station_code`
   - *Metric*: `max_daily_value` (Aggregation: Max)
   - *Insight*: Highlights extreme high values recorded per station, allowing for quick region-based comparisons.
