#!/usr/bin/env python
# coding: utf-8

# # Build an ETL Pipeline
# 
# step1 - Extract Data from Csv file
# 
# step2 - Transform the Data(i.e Clean the Data)- deal with missing and duplicate data
# 
# step3 - Create a Database
# 
# Step4 - Load the clean data into Database

# In[224]:


# Import the libraries
import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
import logging


# In[225]:


# Logging setup
logging.basicConfig(filename="data_pipeline.log", level=logging.INFO)


# In[226]:


# extract the data
Orders = pd.read_csv("F:\Silq\Orders .csv")
Products = pd.read_csv("F:\Silq\Products.csv")
Users = pd.read_csv("F:\\Silq\\Users.csv")


# # EDA

# # Orders

# In[227]:


Orders.head()


# In[228]:


Orders.info()


# In[230]:


# change the datatype of Orders
Orders['OrderDate']=pd.to_datetime(Orders['OrderDate'])


# In[231]:


Orders.info()


# In[241]:


Orders.isnull().sum()


# In[242]:


Orders.duplicated().sum()


# # Products

# In[232]:


Products.head()


# In[234]:


Products.info()


# In[243]:


Products.isnull().sum()


# In[244]:


Products.duplicated().sum()


# # Users

# In[235]:


Users.head()


# In[236]:


Users.info()


# In[238]:


# change the datatype of Orders
Users['SignUpDate']=pd.to_datetime(Users['SignUpDate'])


# In[239]:


Users.info()


# In[245]:


Users.isnull().sum()


# In[246]:


Users.duplicated().sum()


# # Transform the Data- deal with missing and duplicate data
# 
# This dataset does't contain any null value or duplicate values, if there is any null values contains then we can go for Deletion or immutation method

# In[247]:


Orders.drop_duplicates(keep='first').inplace=True


# In[248]:


Orders.dropna(inplace=True)


# In[249]:


Products.drop_duplicates(keep='first').inplace=True


# In[250]:


Products.dropna(inplace=True)


# In[251]:


Users.drop_duplicates(keep='first').inplace=True


# In[252]:


Users.dropna(inplace=True)


# # Create a Database
# 
# Go to PGAdmin(Postgre SQL) and create a Database Table

# In[253]:


# Database Credentials
username = 'Enter your postgresql Username'
password = 'Enter your Password'
host = 'localhost'
port = 5432
db_name ='E-Commerce'


# In[254]:


# Establish a connection
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')

try:
    # Load the data into PostgreSQL database (Table names - orders, products, users)
    Orders.to_sql('orders', engine, if_exists='replace', index=False)
    Products.to_sql('products', engine, if_exists='replace', index=False)
    Users.to_sql('users', engine, if_exists='replace', index=False)
    logging.info('Data pipeline execution completed successfully.')
except Exception as e:
    logging.error(f'Error in data pipeline: {str(e)}')
finally:
    # Close the connection
    engine.dispose()

