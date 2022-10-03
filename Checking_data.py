#!/usr/bin/env python
# coding: utf-8

# In[63]:


import pandas as pd
from dask import dataframe as dd
# start = time.time()


# In[64]:


output_d1=pd.read_csv("/scratch/user/rashmi_1996/Data/final_output_file.csv")
print(output_d1.shape)


# In[65]:


output_d1os=output_d1[["obs_manager","Scoring","file_name"]].drop_duplicates()
del output_d1


# In[ ]:


output_d2=pd.read_csv("/scratch/user/rashmi_1996/final_output_file_total.csv")
print(output_d2.shape)


# In[ ]:


output_d2os=output_d2[["obs_manager","Scoring","file_name"]].drop_duplicates()
del output_d2


# In[ ]:


output_dfinal=pd.concat(output_d1os,output_d2os,axis=0)


# In[ ]:


del output_d1os
del output_d2os


# In[59]:


output_d1os=output_dfinal[["obs_manager","Scoring","file_name"]].drop_duplicates()
output_d1os['Counts'] = output_d1os.groupby(['file_name','Scoring'])['obs_manager'].transform('count')
check_data_output=output_d1os[['file_name','Scoring','Counts']].drop_duplicates().reset_index(drop=True,inplace=True)
check_data_output.to_csv("/scratch/user/rashmi_1996/check_data_output_file.csv")


# In[ ]:


del output_d1os
del check_data_output


# In[22]:


output_d1os=output_dfinal[["obs_manager","Scoring"]].drop_duplicates()
output_d1os['Counts'] = output_d1os.groupby(['Scoring'])['obs_manager'].transform('count')
check_data_output=output_d1os[['Scoring','Counts']].drop_duplicates().reset_index(drop=True)
# check_data_output.to_csv("/scratch/user/rashmi_1996/check_data_output_file.csv")
check_data_output.to_csv("/scratch/user/rashmi_1996/check_data_output_scoring_file.csv")


# In[ ]:


del output_d1os
del check_data_output
del output_dfinal

