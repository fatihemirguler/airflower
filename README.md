ETL Pipeline for Sales Data - Airflow DAG

This project contains an ETL (Extract, Transform, Load) pipeline built with Apache Airflow. The pipeline extracts sales data from a PostgreSQL database and a CSV file, transforms the data by combining, cleaning, and aggregating it, and finally loads the aggregated data into a MySQL database.
Project Structure

    DAG Name: sales_total
    Schedule: Daily (@daily)
    Tasks:
        extract_postgres: Extracts sales data from a PostgreSQL database.
        extract_csv: Extracts sales data from a CSV file.
        transform_data: Combines, cleans, and aggregates the data.
        load_to_mysql: Loads the aggregated sales data into a MySQL database.

Prerequisites
System Requirements:

    Python 3.x
    PostgreSQL database with a table named online_sales
    MySQL database to store the aggregated data

Python Dependencies:

    pandas
    psycopg2
    pymysql
    sqlalchemy
    apache-airflow

DAG Configuration

    Start Date: March 1, 2024
    Retries: 1 on failure
    Email Notifications: Disabled for both failures and retries
    Catchup: Disabled (Runs only for the current day)

Tasks Overview
1. extract_postgres
This task connects to a PostgreSQL database and extracts all rows from the online_sales table. The extracted data is stored as a pandas DataFrame.
2. extract_csv

This task reads sales data from a CSV file located at /Users/fatih/Desktop/school_stuff/mlops/airflow/file.csv and loads it into a pandas DataFrame.
3. transform_data

The task combines the data extracted from both PostgreSQL and CSV sources. It then cleans the data by dropping missing values and converting columns to appropriate data types. The data is further aggregated by summing the quantity and sale_amount per product.
4. load_to_mysql

The aggregated data is inserted into a MySQL table aggregated_sales. If the table already exists, the existing data will be replaced.
Task Dependencies

    Both extract_postgres and extract_csv tasks must complete before the transform_data task starts.
    The load_to_mysql task runs after transform_data completes successfully.
