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



