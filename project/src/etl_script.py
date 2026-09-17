from nt import environ
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pandasql as ps
from dotenv import load_dotenv
import logging
from datetime import datetime
from contextlib import contextmanager

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_PATH = os.path.join(_SCRIPT_DIR, 'etl_log.log')
logging.basicConfig(
    filename=_LOG_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True,
)
CSV_FILE_PATH = os.getenv('CSV_FILE_PATH')
PORT = os.getenv('PORT')
DB_NAME = os.getenv('DB_NAME')
HOST = os.getenv('HOST')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

print("Environment variables set.")
class FileEmptyError(Exception):
     pass
class TransformationError(Exception):
    pass
class Load_Data_Error(Exception):
    pass

@contextmanager
def get_db_connection():
    connection=psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"] ,
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"]
    )
    try:
        yield connection
    except Exception as e:
        logging.error(f"Error connectng to the database..%s",e,exc_info=True)
        connection.rollback()
        raise
    finally:
        logging.info("Closing Database Connection")
        connection.close()    
def extract_data(CSV_FILE_PATH):
    try:
        logging.info("Extracting data from file: %s", CSV_FILE_PATH)
        print(f"------Extracting data from file: {CSV_FILE_PATH} at time: {datetime.now()}----")
        df = pd.read_csv(CSV_FILE_PATH)
        if df is None or df.empty:
            logging.error("No data could be extracted: file is empty or not found")
            print(f"------File is empty or not found at time: {datetime.now()}----")
            raise FileEmptyError("File is empty or not found")

        logging.info("Data extracted successfully (%s rows, %s columns)", len(df), len(df.columns))
        print(f"------Data Extracted Successfully at time: {datetime.now()}----")
        print(df.head(5))
        print("--------------------------------")
        return df

    except Exception as e:
        logging.error("Error extracting data: %s", e, exc_info=True)
        return None
def print_schema(df):
    schema={
        "Columns":{col:df[col].dtype for col in df.columns},
        "Size":df.shape
        
    }
    print("Schema: ",schema)
    null_values={
        "Columns":{col:df[col].isna().sum() for col in df.columns},
    }
    print("Null Values: ",null_values)
    print("--------------------------------")
def transform_data(df):
    try:
        print(f"------Transforming data at time: {datetime.now()}----")
        print(df.head(5))
        print("Shape of the dataframe: ",df.shape)
        df_duplicates=df[df.duplicated()]
        df_pos_amount=df.filter(df['total_amount']>0)
        df_neg_amount=df.filter(df['total_amount']<0)
        print(f"------Number of rows: {df.shape[0]} and number of columns: {df.shape[1]} at time: {datetime.now()}----")
        print("Number of duplicates: ",df_duplicates.shape[0])
        print("Number of positive amount: ",df_pos_amount.shape[0])
        print("Number of negative amount: ",df_neg_amount.shape[0])
        df["billing_date"]=pd.to_datetime(df["billing_date"],format="mixed",errors="coerce")
        df["category"]=df["category"].fillna("Others")
        df["discount"]=df["discount"].fillna(0.0)
        df["total_amount"]=df["total_amount"].abs()
        df["quantity"]=df["quantity"].abs()
        print(f"Transformation Job Completed Successfully {datetime.now()}")
        
        print("--------------------------------")
        df["billing_date"]=df["billing_date"].values.tolist()
        
        return df
    except Exception as e:
        logging.error("Error transforming data: %s", e, exc_info=True)
        raise TransformationError("Error transforming Data")
        return None
def load_staging_table(df, db_conn, staging_table="public.staging_sales"):
    try:
        with db_conn.cursor() as cursor:
            cursor.execute(f"""
                DROP TABLE IF EXISTS {staging_table};
                CREATE TABLE {staging_table} (
                    item_id TEXT,
                    item_name TEXT,
                    billing_date DATE,
                    total_amount NUMERIC,
                    quantity NUMERIC,
                    category TEXT,
                    discount NUMERIC
                );
            """)
        cols = ["item_id", "item_name", "billing_date", "total_amount", "quantity", "category", "discount"]
        rows = list(df[cols].itertuples(index=False, name=None))
        with db_conn.cursor() as cursor:
            execute_values(cursor, f"INSERT INTO {staging_table} ({','.join(cols)}) VALUES %s", rows)
        db_conn.commit()
        logging.info("Staging table %s loaded with %s rows", staging_table, len(rows))
    except Exception as e:
        logging.error("Error loading staging table: %s", e, exc_info=True)
        raise Load_Data_Error(f"Error loading staging table: {datetime.now()}")

def load_daily_sales_data(df):
    try:
        print(f"------Loading daily sales data at time: {datetime.now()}----")
        target_daily_sales_table_name=os.getenv('TARGET_DAILY_SALES_TABLE')
        query= f"""
        MERGE INTO {target_daily_sales_table_name} AS target_daily_sales_revenue 
        USING (
        SELECT CAST(billing_date AS DATE) as billing_date, sum(total_amount) as total_daily_revenue 
        from public.staging_sales
        group by CAST(billing_date AS DATE) 
        order by billing_date ) AS source
        ON target_daily_sales_revenue.billing_date=source.billing_date
        WHEN MATCHED THEN
        UPDATE set total_daily_revenue=source.total_daily_revenue
        WHEN NOT MATCHED THEN
        INSERT (billing_date,total_daily_revenue) VALUES (source.billing_date,source.total_daily_revenue)
        """
        with get_db_connection() as db_conn:
            load_staging_table(df,db_conn)
            with db_conn.cursor() as cursor:
                cursor.execute(query)
            db_conn.commit()

        logging.info(f"ETL Job Refresh Table {target_daily_sales_table_name} at {datetime.now()}")
        print(f"---Daily Sales Data Updated Successfully at {datetime.now()}---")
        
    except Exception as e:
        logging.error(f"Error loading daily sales data: %s", e, exc_info=True)
        raise Load_Data_Error(f"Error Loading Data: {datetime.now()}")
        return None

def load_monthly_sales_data(df):
    try:
        target_monthly_sales_table=os.environ["TARGET_MONTHLY_SALES_TABLE"]
        query=f"""
    MERGE INTO {target_monthly_sales_table} as target_monthly_sales
    USING (
    SELECT TO_CHAR(billing_date,'YYYY-MMMM') as month,sum(total_amount) as total_sales   FROM public.staging_sales
    group by TO_CHAR(billing_date,'YYYY-MMMM')
    order by month
    ) AS source_monthly_sales
    on target_monthly_sales.month=source_monthly_sales.month
    WHEN MATCHED THEN
    UPDATE SET total_sales=source_monthly_sales.total_sales
    WHEN NOT MATCHED THEN
    INSERT (month,total_sales) VALUES(source_monthly_sales.month,source_monthly_sales.total_sales);
    """
        with get_db_connection() as db_conn:
            load_staging_table(df,db_conn)
            with db_conn.cursor() as cursor:
                cursor.execute(query) 
            db_conn.commit()
        logging.info(f"ETL Job Refresh Table {target_monthly_sales_table} successfully at {datetime.now()}")
        print("------------------- ")
    except Exception as e:
        logging.error(f"Error loading monthly sales revenue %s",e, exc_info=True)   
        raise Load_Data_Error(f"Error Loading Data: {datetime.now()}")

def load_top_three_revenue_items(df):
    try:
        target_top_three_items=os.environ["TARGET_TOP_ITEMS_BY_REVENUE_TABLE"]
        query=f"""
        truncate table {target_top_three_items};
        insert into {target_top_three_items} (item_id,item_name,total_revenue,item_revenue_rank) 
        with total_revenue_by_items as (
        
        select item_id,item_name,sum(total_amount) as total_revenue
           from public.staging_sales
           group by item_id,item_name
        
        
        
        ),
        rank_items as (
        select item_id,item_name,total_revenue,row_number() over(order by total_revenue desc) as item_revenue_rank from total_revenue_by_items
        
        )
        select item_id,item_name,total_revenue,item_revenue_rank from rank_items
        where item_revenue_rank<=3
        order by item_revenue_rank
        
        
         """

        with get_db_connection() as db_conn:
            load_staging_table(df,db_conn)
            with db_conn.cursor() as cursor:
                 cursor.execute(query)
            db_conn.commit()
        print("-----------")
        logging.info(f"ETL Job Refresh Table {target_top_three_items} successfully at {datetime.now()}")

    except Exception as e:
        logging.error(f"Error loading top revenue items %s",e,exc_info=True)    
        raise Load_Data_Error(f"Error Loading Data: {datetime.now()}")
def main():
    logging.info("ETL job started")
    df = extract_data(CSV_FILE_PATH)
    if df is not None:

        logging.info("ETL extract phase completed successfully")
        print_schema(df)
        data=transform_data(df)
        load_daily_sales_data(data)
        load_monthly_sales_data(data)
        load_top_three_revenue_items(data)
        print_schema(data)
    else:
        logging.error("ETL extract phase failed")
        
if __name__ == "__main__":
    main()
