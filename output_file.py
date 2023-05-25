#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# In[3]:


spark = SparkSession\
        .builder\
        .config('spark.driver.bindAddress', '127.0.0.1')\
        .appName('sample-spark')\
        .getOrCreate()


# In[40]:


customers_data_path = './input_data/starter/customers.csv'
product_data_path = './input_data/starter/products.csv'
transactions_data_path = './input_data/starter/transactions/'


# In[41]:


customers_data = spark.read.option('header','true').csv(customers_data_path)
product_data = spark.read.option('header','true').csv(product_data_path)
transactions_data = spark.read.option('header','true').json(transactions_data_path)


# In[38]:


customers_transactions_data = customers_data.join(transactions_data, ['customer_id'], 'inner')

joined_customers_product_transactions = customers_transactions_data.join(product_data)\
    .groupBy('customer_id', 'loyalty_score', 'product_id', 'product_category')\
    .agg(count('*').alias('purchase_count'))


# In[39]:


joined_customers_product_transactions.write.mode('append').json('output_json')


# In[ ]:




