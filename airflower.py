import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import pymysql
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    dag_id='sales_total',
    default_args=default_args,
    description='An ETL pipeline to extract, transform, and load sales data.',
    schedule='@daily',
    catchup=False
)

# Define extract, transform, and load functions

def extract_postgres():
    # PostgreSQL connection details
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="qaresma1",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    extract_query = """
    SELECT * FROM online_sales
    """
    cursor.execute(extract_query)
    rows = cursor.fetchall()

    cursor.close()

    column_names = ['sales_id', 'product_id', 'quantity', 'sale_amount', 'sale_date']
    df = pd.DataFrame(rows, columns=column_names)
    return df

def extract_csv():
    # Read CSV file
    df = pd.read_csv('/Users/fatih/Desktop/school_stuff/mlops/airflow/file.csv')
    return df

def transform_data(postgres_data, csv_data):
    # Combine data from both sources
    combined_data = pd.concat([postgres_data, csv_data], ignore_index=True)

    # Data cleansing
    cleaned_data = combined_data.dropna()
    cleaned_data['sale_amount'] = cleaned_data['sale_amount'].astype(float)

    # Aggregate data
    aggregated_data = cleaned_data.groupby('product_id').agg({'quantity': 'sum', 'sale_amount': 'sum'}).reset_index()
    return aggregated_data

def load_to_mysql(aggregated_data):
    # MySQL connection details
    host = 'localhost'
    user = 'root'
    password = 'qaresma1'
    database = 'airflow'
    table = 'aggregated_sales'

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
    connection = engine.connect()

    # Load data into MySQL table
    aggregated_data.to_sql(table, con=engine, if_exists='replace', index=True)

    connection.close()

# Define Airflow tasks
extract_postgres_task = PythonOperator(
    task_id='extract_postgres',
    python_callable=extract_postgres,
    dag=dag
)

extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'postgres_data': extract_postgres(), 'csv_data': extract_csv()},
    dag=dag
)

load_to_mysql_task = PythonOperator(
    task_id='load_to_mysql',
    python_callable=load_to_mysql,
    op_kwargs={'aggregated_data': transform_data(extract_postgres(), extract_csv())},
    dag=dag
)

# Define task dependencies
extract_postgres_task >> transform_task
extract_csv_task >> transform_task
transform_task >> load_to_mysql_task
