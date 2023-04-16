#!/usr/bin/env python
# coding: utf-8

# In[1]:


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import wget
import zipfile
import psycopg2
# Define Postgres connection details
pg_host = 'localhost'
pg_database = 'Customer_Subscription'
pg_user = 'postgres'
pg_password = 'Nevergiveup.1'


default_args = {
    'owner': 'XYZ Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

def extract_data():
    # extract data from CSV files
    wget.download('https://archive.org/download/airflow/airflow.zip')
    unzipcsvfile = zipfile.ZipFile('./airflow.zip')

    # load the CSV data into Pandas dataframes for later transformation
    customer_data_df = pd.read_csv(unzipcsvfile.open('customer_data.csv'))
    order_data_df = pd.read_csv(unzipcsvfile.open('order_data.csv'))
    payment_data_df = pd.read_csv(unzipcsvfile.open('payment_data.csv'))

def transform_data(): 
    # convert date fields to the correct format using pd.to_datetime
    # merge customer and order dataframes on the customer_id column
    # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
    # drop unnecessary columns like customer_id and order_id
    # group the data by customer and aggregate the amount paid using sum
    # group the data by customer and aggregate the amount paid using sum andcreate a new column to calculate the total value of orders made by each customer
    # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 
    
    
    # convert date fields to the correct format using pd.to_datetime
    customer_data_df['date_of_birth'] = pd.to_datetime(customer_data_df['date_of_birth'])
    order_data_df['order_date'] = pd.to_datetime(order_data_df['order_date'])
    payment_data_df['payment_date'] = pd.to_datetime(payment_data_df['payment_date'])
     # merge customer and order dataframes on the customer_id column
        
    customer_df = pd.merge(customer_data_df, order_data_df, how="left", on=["customer_id"])
    
    # merge payment dataframe with the merged dataframe on the order_id and customer_i"d columns
    merged_df = pd.merge(customer_df, payment_data_df, how="left", on=["customer_id", "order_id"])
    
    # drop unnecessary columns like customer_id and order_id
    new_merged_df =merged_df.drop(['customer_id', 'order_id'], axis=1)
    
    # group the data by customer and aggregate the amount paid using sum andcreate a new column to calculate the total value of orders made by each customer
    transformed_data    = new_merged_df.groupby(['first_name', 'last_name'], as_index=False).agg(
        amount_paid =pd.NamedAgg(column="amount", aggfunc="sum"),
        total_order_value=pd.NamedAgg(column="price", aggfunc="sum"))
    
def load_data():
        # create engine
    engine = create_engine('postgresql+psycopg2://postgres:Nevergiveup.1@localhost:5432/MTN')

   #load data to postgress
    transformed_data.to_sql('MTN_Customer_Orders', con=engine, if_exists='replace', index=False)


    
with dag:
     # extract data 
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )
    
    # transform data 

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    # load data 

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    extract_task >> transform_task >> load_task


# In[ ]:




