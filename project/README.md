### Project Structure
```
project/
├── data/
│   └── assignment_dataset.csv
├── docker/
│   └── docker-compose.yml
├── src/
│   ├── etl_script.py
│   ├── etl_log.log
│   └── .env
└── README.md
```
### ETL Script
The python script for the ETL pipeline is etl_script.py.
The entire pipeline is idempotent and uses merge-upsert approach to handle data in the 3 tables. There is also a staging table to store all the principal data
is divided into following functions
1. extract: Data is extracted from the CSV File
2. transform: Data is transformed into pandas Dataframe with clean, accurate and valid values without duplicates
3. load_staging_tables: This is where the results from the transformed dataframe are stored
4. load_daily_sales_data: This loads daily sales data using merge upsert approach to avoid duplicates
5. load_monthly_sales_data:This loads monthly sales data using merge upsert approach to avoid duplicates and error in the way data is stored
6. load_top_three_revenue_items: This loads top 3 revenue items with merge upsert based technique

### Define Postgres Service in Docker using docker-compose.yml file
```
services:
  postgres-db:
    image: postgres:15
    container_name: postgres-db
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: "####"
      POSTGRES_DB: sales_pipeline_db
    ports:
      - "5432:5432"

```

### Setup Instructions
Create a docker-compose.yml file inside project\docker folder
with all the important details like database name,username,password,port and give it a name as postgres-db
### How to start the service
### 1. Start the postgres Container
```
a. Go the folder docker using
cd project\docker
b. Then start the service using docker compose command
docker compose up -d
c. Confirm if docker container is running or not
docker ps
```
### 2. Run the Python Script
```
a. Direct to the src folder 
   cd project\src
b. Run the script
   python etl_script.py
```

### 3. Check Logs
a. Check etl_log.log for logs
```powershell
type etl_log.log
```
### 4. Stopping the database container
```powershell
cd project\docker
docker compose down
```



