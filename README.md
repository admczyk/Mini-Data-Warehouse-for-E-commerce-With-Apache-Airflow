# Mini Data Warehouse For E-commerce with Apache Airflow
This project is a complete ETL (Extract, Transform, Load) pipeline that fetches weather data from the __Website__, processes it and loads it into a PostgreSQL database.
The pipeline is orchestrated using Apache Airflow.

## Project Overview
This project implements a mini data warehouse for e-commerce data using Apache Airflow.

The pipeline extracts product data from a public API, transforms it into an analytical format, and loads it into a PostgreSQL database.

The dataset includes:
- product information (id, title, price, description, category)
- customer ratings (rate, count)
- segmented features (price_category, rating_category, product_segment)
- aggregated variables (avg_price_category, avg_rating_category)

The goal of the project is to simulate a real-world data engineering workflow, including:
- ETL pipeline design
- data cleaning and validation
- feature engineering for analytics
- orchestration with Apache Airflow

## Project Structure
```
mini_data_warehouse/
|
├── dags/                   # Contains Apache Airflow DAGs responsible for orchestrating the workflow
|   ├── create_table.py     # Initializes database schema and tables
|   └── etl_pipeline.py     # Defines the ETL process and task dependencies
├── data/                   # Stores data used across the pipeline
|   ├── raw                 # Raw data extracted from external API (JSON format)
|   └── transformed         # Cleaned and processed data ready for loading (CSV format)
├── etl/                    # Implements the core ETL logic
|   ├── extract.py          # Handles data retrieval from external sources
|   ├── load.py             # Loads processed data into PostgreSQL database
|   └── transform.py        # performs data cleaning, validation and feature engineering
├── .env                    # Environmental variables used for configuration
├── docker-compose.yaml     # Defines the containerized environment, includeing Airflow services and PostgreSQL database
├── key_generator.py        # Generates FERNET_KEY and SECRET_KEY
├── requirements.txt        # Lists Python dependencies required to run the project
└── README.md               # Project documentation
```

## Technologies used
- SQL
- PostgreSQL
- Python (ETL scripts)
- Docker
- Git / Github

## Airflow DAGs
The project uses Apache Airflow to orchestrate data workflows through Directed Acyclic Graphs (DAGs). Each DAG defines a sequence of tasks and their dependencies, enabling automated and repeatable data processing.
- __create_table.py__ - this DAG is responsible for initializing the database schema
- __etl_pipeline.py__ - this is the main DAG that implements the end-to-end ETL pipeline

## ETL Pipeline
The ETL process consists of:
#### 1. Extract
- Fetches data using HTTP requests
- Retrieves raw data from external API
- Saves raw data as JSON file for traceability

#### 2. Transform
- Data is read from JSON file using Pandas
- Nested fields are flattened
- Handles missing values
- Data types are explicitly cast
- Additional analytical columns are created
- Saves transformed data as CSV for traceability

#### 3. Load
- Data is read from CSV using Pandas
- Connection is managed via Airflow PostgresHook
- Records are inserted row-by-row into the products table

## Setup Instructions
#### 1. Clone the Repository
```
git clone https://github.com/admczyk/Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow.git
cd Mini-Data-Warehouse-for-E-commerce-With-Apache-Airflow
```
#### 2. Create a Virtual Environment
Ensure you have Python installed, then create and activate a virtual environment:
```
python -m venv venv
source venv/bin/activate    # On macOS/Linux
venv\Scripts\activate       # On Windows
```
#### 3. Install Dependencies
Install the required Python packages:
```
pip install -r requirements.txt
```
#### 4. Set Up Environment Variables
Create `.env` file in the root directory and assign values:
```
POSTGRES_USERNAME= 
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DATABASE_NAME=

AIRFLOW_UID=                            # 1000 on macOS/Linux or 50000 on Windows
AIRFLOW_WWW_USER_USERNAME=
AIRFLOW_WWW_USER_PASSWORD=
FERNET_KEY=
SECRET_KEY=

WEBSITE_PATH="https://fakestoreapi.com/"
CATEGORY='products'
```
`FERNET_KEY` and `SECRET_KEY` should be generated. To do this run `key_generator.py` file.

#### 5. Build and Start Services
Run Docker compose to start Airflow and PostgreSQL
```
docker compose up -d
```
This command will start:
- Airflow webserver
- Airflow scheduler
- PostgreSQL database

#### 6. Access Airflow UI
Open browser and go to:
```
http://localhost:8080
```
or _(if above doesn't work)_:
```
http://127.0.0.1:8080
```
Login using username and password set in environment variables.

#### 7. Run the Pipeline
1. Enable the DAG `create_product_table`
2. Trigger it manually to create SQL table
3. Enable the DAG `etl_pipeline`
4. Trigger it manually or wait for the scheduler

#### 8. Verify Results
- Raw data - `data/raw/`
- Transformed data - `data/transformed/`
- Database table:
    ```
    docker exec -it postgres bash

    psql -U YOUR_POSTGRES_USERNAME -d YOUR_POSTGRES_DATABASE

    SELECT * FROM products LIMIT(10);
    ```
### Stopping the Environment
To stop all services use:
```
docker compose down
```
### Reseting the Environment
To remove all data and start fresh use:
```
docker compose down -v
```
## Future Improvements
- Add support for multiple data sources (users, carts, etc.)
- Integrate BI tools (Power BI / Tableau)
- Deploy pipeline to cloud
