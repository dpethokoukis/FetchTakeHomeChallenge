#!/usr/bin/env python
# coding: utf-8

# In[ ]:


########################################################################
## Name: Deirdre Pethokoukis
## Script Name: 02b_FetchTakeHome_DataQualityChecks_Python
## Purpose: Graph data to check for data quality in 3 tables
##          Transaction, Users, and Products
################################################################################


# In[ ]:


#####################
##  LOAD PACKAGES  ##
#####################


# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# In[ ]:


#################
##  LOAD DATA  ##
#################


# In[ ]:


####1. Products Data


# In[2]:


#Import using pandas read.csv, import all as strings to avoid losing information
raw_products_df = pd.read_csv(r"C:\Users\deirdre.pethokoukis\PRODUCTS_TAKEHOME.csv",dtype = "str")


# In[3]:


#Ensure imported correctly - rows in csv: 845552, columns in csv: 7 -- matches results
raw_products_df.shape


# In[ ]:


####2. Transaction Data


# In[4]:


#Import using pandas read.csv, import all as strings to avoid losing information
raw_transaction_df = pd.read_csv(r"C:\Users\deirdre.pethokoukis\TRANSACTION_TAKEHOME.csv",dtype = "str")


# In[5]:


#Ensure imported correctly - rows in csv: 50000, columns in csv: 8 -- matches results
#Could be suspicious that exactly 50,000 - is datset complete?
raw_transaction_df.shape


# In[ ]:


####3. User Data


# In[6]:


#Import using pandas read.csv, import all as strings to avoid losing information
raw_user_df = pd.read_csv(r"C:\Users\deirdre.pethokoukis\USER_TAKEHOME.csv",dtype = "str")


# In[7]:


#Ensure imported correctly - rows in csv: 100000, columns in csv: 6 -- matches results
#Could be suspicious that exactly 100,000 - is datset complete?
raw_user_df.shape


# In[ ]:


#####################################
##  Investigate Table for Staging  ##
##  and Data Quality               ##
####################################
#Notes: using relationship entity model for datatypes


# In[ ]:


####1. Transaction Data


# In[8]:


#a. Purchase Date Outliers
#Convert PURCHASE_DATE column to datetime2
raw_transaction_df['PURCHASE_DATE'] = pd.to_datetime(raw_transaction_df['PURCHASE_DATE'])

#Get number of records for each PURCHASE_DATE
date_counts = raw_transaction_df['PURCHASE_DATE'].value_counts().sort_index()

#Plot the graph
plt.figure(figsize=(10, 6))
#Specify data for each axis
plt.bar(x=date_counts.index, height = date_counts.values)
#Add labels/titles
plt.xlabel('Purchase Date')
plt.ylabel('Count of Records')
plt.title('Number of Records per Purchase Date')
#Rotate the x-axis 45 degrees so easier to read
plt.xticks(rotation=45)  
#Shoe graph
plt.show()

#No dates with way more data or falling way outside of range.


# In[9]:


#b. FINAL_SALE Outliers
#Convert FINAL_SALE to float (convert BLANKs to NULLs)
raw_transaction_df['FINAL_SALE'] = raw_transaction_df['FINAL_SALE'].replace(' ', np.nan).astype(float)

#Get rid of NULLs
raw_transaction_df_NONULLs = raw_transaction_df.dropna(subset=['FINAL_SALE'])

#Plot the box and whisker plot
plt.figure(figsize=(10, 6))
plt.boxplot(raw_transaction_df_NONULLs['FINAL_SALE'], vert=False)
plt.title('Final Sale Outliers')
plt.xlabel('Final Sale')
plt.grid(True)

# Show the plot
plt.show()

#The data is very concentrated around zero. Would need to do further analysis to get more information about outliers.


# In[ ]:


####2. USERS DATA


# In[10]:


#a. Counts by State
#Get number of records for each STATE
date_counts = raw_user_df['STATE'].value_counts().sort_values(ascending=False)

#Plot the graph
plt.figure(figsize=(10, 6))
#Specify data for each axis
plt.bar(x=date_counts.index, height = date_counts.values)
#Add labels/titles
plt.xlabel('State')
plt.ylabel('Count of Records')
plt.title('Number of Records per State')
#Rotate the x-axis 90 degrees so easier to read
plt.xticks(rotation=90)  
#Shoe graph
plt.show()

#Bigger states have more records than smaller/less populated states.


# In[ ]:


####3. PRODUCTS Data


# In[11]:


#a. CATEGORY_1 Outliers
#Get number of records for each CATGORY_1
date_counts = raw_products_df['CATEGORY_1'].value_counts().sort_values(ascending=False)

#Plot the graph
plt.figure(figsize=(10, 6))
#Specify data for each axis
plt.bar(x=date_counts.index, height = date_counts.values)
#Add labels/titles
plt.xlabel('Category 1')
plt.ylabel('Count of Records')
plt.title('Number of Records per Category 1')
#Rotate the x-axis 90 degrees so easier to read
plt.xticks(rotation=90)  
#Shoe graph
plt.show()

#Health&Wellness and Snacks have way more records than other categories.

