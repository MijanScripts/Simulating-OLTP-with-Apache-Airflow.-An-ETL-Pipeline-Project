#Import the libraries 
#!pip install --force-reinstall ibm_db==3.1.0 ibm_db_sa==0.3.3
# Ensure we don't load_ext with sqlalchemy>=1.4 (incompadible)
#!pip uninstall sqlalchemy==1.4 -y && pip install sqlalchemy==1.3.24
#!pip install ipython-sql

import ibm_db

from datetime import timedelta

#import the DAG instance
from airflow import DAG
#import the operator 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#import scheduler 
from airflow.utils.dates import days_ago
#Python libraries
import os 
import csv
import json
#import pandas as pd

#DAG arguments 
default_args = {
    'owner':'SOLTIC',
    'start_date': days_ago(0),
    'email': ['solticmijan93@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0,
    'retries_on_delay' : timedelta(minutes=5),
    'max_active_runs_per_dag' : 1,
    'depends_on_past': False 
}

# Dag Definition and instantiation
dag = DAG(
    'READROWS',
    default_args = default_args,
    description = 'Read_rows_from_a_csv_file_and_append_to_db2',
    schedule_interval = timedelta(minutes=2),
    catchup = False
)


def update_json():
    json_file = 'last_row_index.json'

    # Load the last row index from the JSON file, and if JSON file deos not exist, create a new one and set index to 0
    # This function increases the index number in the JSON file by +1 every time this function is run
    # Making it possible to read a new row index
    if os.path.isfile(json_file):
        try:
            with open(json_file, 'r') as f:
                last_row_index = json.load(f)
        except json.decoder.JSONDecodeError:
            last_row_index = 0
            with open(json_file, 'w') as f:
                json.dump(last_row_index, f)
    else:
        last_row_index = 0
        with open(json_file, 'w') as f:
            json.dump(last_row_index, f)
    return last_row_index

def get_new_rows():
    #This function reads the input from a JSON file, which is an index number
    #reads the current row that matches with the index number in the JSON file
    last_row_index = update_json()
    
    json_file = 'last_row_index.json'
    # Open the CSV file
    with open('/home/project/billing.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)

        # Skip rows that have already been read
        for _ in range(last_row_index):
            next(reader)

        # Read the next row
        try:
            rows = next(reader)
            row=[]
            #This for loop transform each element in the role to fit our data model in the target table
            for i,n in enumerate(rows):
                if i==0 or i==-1:
                    row.append(int(n))
                else:
                    row.append(n)

            # Update the last row index in the JSON file
            with open(json_file, 'w') as f:
                json.dump(last_row_index + 1, f)
        except StopIteration:
            # End of file reached, reset last row index to 0
            with open(json_file, 'w') as f:
                last_row_index = 0
                json.dump(last_row_index, f)
    return row


# Db2 Service Credentials
def connect_to_db():
    dsn_driver = "{IBM DB2 ODBC DRIVER}"
    dsn_database = "bludb"            # e.g. "BLUDB"
    dsn_hostname = "ba99a9e6-d59e-4883-8fc0-d6a8c9f7a08f.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"            # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
    dsn_port = "31321"                    #Port Number e.g. "50000" 
    dsn_protocol = "TCPIP"            # i.e. "TCPIP"
    dsn_uid = "bsl21284"                 # UserID e.g. "abc12345"
    dsn_pwd = "AV9tEC0bjkWxtFqw"                 # Password e.g. "7dBZ3wWt9XN6$o0J"
    dsn_security = "SSL"              #i.e. "SSL"
    
    dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

    try:
        conn = ibm_db.connect(dsn, "", "")
        print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

    except:
        print ("Unable to connect: ", ibm_db.conn_errormsg() )
    
    return conn



def insert_data(row):
    # Define the SQL query with placeholders for the data
    query = "INSERT INTO billing_data (CUSTOMERID, CATEGORY, COUNTRY, INDUSTRY, MONTH, BILLEDAMOUNT) VALUES (?,?,?,?,?,?)"

    # Define the tuple with the row data
    row = tuple(row)

    #Connecting to the database
    conn = connect_to_db()

    # Prepare the statement
    stmt = ibm_db.prepare(conn, query)

    # Bind the tuple values as parameters
    for i, value in enumerate(row):
        ibm_db.bind_param(stmt, i + 1, value)

    # Execute the statement
    ibm_db.execute(stmt)

    # Commit the transaction to save the changes
    ibm_db.commit(conn)

    # Close connection after saving changes
    ibm_db.close(conn)


extract_transform = PythonOperator(
    task_id='extract_transform',
    python_callable=get_new_rows,
    dag=dag,
)

load = PythonOperator(
    task_id = 'load_new_rows',
    python_callable = insert_data,
    dag = dag,
    op_kwargs= {'row':'{{task_instance.xcom_pull(task_ids="extract_transform")}}'}
)

extract_transform >> load 
