#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql import functions as F


# In[2]:


conf1 = (SparkConf().setMaster("yarn").setAppName("s4604001_ass_process_result"))
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("s4604001_ass_process_result_session").getOrCreate()


# In[3]:


pd.set_option('display.max_columns', 400)
pd.set_option('display.max_rows', 400)


# In[4]:


# read files
df_07 = spark.read.options(header='True').csv("/user/s4604001/ass/result_07").toPandas()
df_08 = spark.read.options(header='True').csv("/user/s4604001/ass/result_08").toPandas()
df_09 = spark.read.options(header='True').csv("/user/s4604001/ass/result_09").toPandas()
df_10 = spark.read.options(header='True').csv("/user/s4604001/ass/result_10").toPandas()
df_11 = spark.read.options(header='True').csv("/user/s4604001/ass/result_11").toPandas()
df_12 = spark.read.options(header='True').csv("/user/s4604001/ass/result_12").toPandas()


# In[5]:


# count the length of one file
df_08.count()


# In[6]:


# merge all the files
df = df_07.append(df_08)
df = df.append(df_09)
df = df.append(df_10)
df = df.append(df_11)
df = df.append(df_12)
df = df.rename(columns = {'col': 'drink_type'})
df = df.astype({'count': 'int32'})
df.groupby(['drink_type', 'lang', 'utc_offset', 'hour'], as_index=False).sum()


# In[7]:


# count the number of tweets in ten languages no matter mentioned drink
df_num = df['count'].sum(axis = 0)
df_num


# In[8]:


# count the number of tweets in ten languages mentioned drink
df_notnull = df[~df['drink_type'].isna()]
df_notnull_num = df_notnull['count'].sum(axis = 0)
df_notnull_num


# In[9]:


# count the number of tweets mentioned different drink type
df_notnull.groupby(['drink_type']).sum()


# In[10]:


# compute the percentage of tweets mentioned drink in different language
df_lang = df.groupby(['lang']).sum()
df_lang_notnull = df_notnull.groupby(['lang']).sum()
df_lang_per = pd.merge(df_lang_notnull, df_lang, how='inner', on='lang')
df_lang_per['percentage'] = df_lang_per['count_x'] / df_lang_per['count_y']
df_lang_per = df_lang_per.rename(columns = {'count_x': 'lang_num', 'count_y': 'num'})
df_lang_per = df_lang_per.sort_values('percentage', ascending=0)
df_lang_per


# In[11]:


# compute the percentage of tweets mentioned different drink type in different language
df_lang_tmp = df_lang_per.sort_values(['lang'], ascending=[0])
df_tmp = df_notnull.groupby(['lang', 'drink_type'], as_index=False).sum().sort_values(['lang'], ascending=[0])
df_lang_drink = pd.merge(df_lang_tmp, df_tmp, how='right', on='lang').drop(['lang_num', 'percentage'], axis=1)
df_lang_drink['percentage'] = df_lang_drink['count'] / df_lang_drink['num']
df_lang_drink = df_lang_drink[['lang', 'drink_type', 'count', 'num', 'percentage']].sort_values(['drink_type', 'percentage'], ascending=[0, 0])
df_lang_drink


# In[12]:


# count the number of tweets mentioned different drink type in a day
df_hour_notnull = df_notnull[~df_notnull['utc_offset'].isna()]
df_hour_notnull_num = df_hour_notnull['count'].sum(axis = 0)
df_hour_notnull = df_hour_notnull.astype({'utc_offset': 'int32', 'hour': 'int32'})
df_hour_notnull['utc_os_hour'] = np.floor(df_hour_notnull['utc_offset'] / 3600)
df_hour_notnull['true_hour'] = (df_hour_notnull['hour'] + df_hour_notnull['utc_os_hour']) % 24
df_hour_notnull = df_hour_notnull.astype({'true_hour': 'int32'})
df_hour_notnull = df_hour_notnull[['drink_type', 'true_hour', 'count']].groupby(['drink_type', 'true_hour'], as_index=False).sum().sort_values(['drink_type', 'true_hour'], ascending=[0, 1])
df_hour_notnull


# In[13]:


# count the number of tweets in a day
df_hour = df[~df['utc_offset'].isna()]
df_hour_num = df_hour['count'].sum(axis = 0)
df_hour = df_hour.astype({'utc_offset': 'int32', 'hour': 'int32'})
df_hour['utc_os_hour'] = np.floor(df_hour['utc_offset'] / 3600)
df_hour['true_hour'] = (df_hour['hour'] + df_hour['utc_os_hour']) % 24
df_hour = df_hour.astype({'true_hour': 'int32'})
df_hour = df_hour[['true_hour', 'count']].groupby(['true_hour']).sum().sort_values(['true_hour'], ascending=[1])
df_hour


# In[14]:


# compute the percentage of tweets mentioned different drink type in a day
df_hour_per = pd.merge(df_hour_notnull, df_hour, how='inner', on='true_hour')
df_hour_per['percentage'] = df_hour_per['count_x'] / df_hour_per['count_y']
df_hour_per = df_hour_per[['drink_type', 'true_hour', 'percentage']].groupby(['drink_type', 'true_hour'], as_index=False).sum().sort_values(['drink_type', 'true_hour'], ascending=[0, 1])
df_hour_per


# In[15]:


# save the reslut of time
df_hour_per.to_csv("drink_type_hour.csv", header=True, index=False, sep=',')


# In[16]:


# save the reslut of language
df_lang_drink.to_csv("drink_type_lang.csv", header=True, index=False, sep=',')

