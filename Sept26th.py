#!/usr/bin/env python
# coding: utf-8

# In[33]:


#TODO
#IGNORE LASTNAME_ONLY COLUMN -- ONE WORD IS LASTNAME
# 1. 340 first and last names --> .dta files --> fundfamilystate (not prior)
# 2. Match with seller name or owner name --> middle name initial match if middle name initial exists on both (full).
# 3. check threshold (relax 0.01) and check matching (with 400k)
# 4. Give scoring --> based on existence --> initial check (for middle names) /// add column with name of matchtype
# 5.OUTPUT FILE --> For each row of the manager data, we find the potential linked CoreLogic IDs5.
# CoreLogic IDs, names in corelogic that is matched


# In[34]:

from datetime import datetime
start_time = datetime.now()


# In[35]:
# %pip install pandas

import pandas as pd
import gc
# import dask.dataframe as dd
# from scipy import spatial
import numpy as np
# import dask.array as da
import os
import re


# In[36]:


def get_name(s,type):
    name=s.split(" ")
    length=len(name)
    if type=="last":
        return name[-1].strip()
    elif type=="first" and length==1:
        return ""
    elif type=="first" and length>=2:
        val=name[0].replace(".","").strip()
        if len(val)==1 and length>2:
            return name[1].strip()
        elif len(val)==1 and length==2:
            return ""
        else:
            return name[0].strip()
    elif type=="middle" and length==3:
        val1=name[0].replace(".","").strip()
        if len(val1)==1:
            return val1
        else:
            middle_name=name[1].replace(".","").strip()
            return middle_name
    elif type=="middle" and length==2:
        val=name[0].replace(".","").strip()
        if len(val)==1 and length==2:
            return val
        else:
            return ""
    else:
        return ""
    
def preprocess_manager_data(manager_data):
    print("shape before preprocessing",manager_data.shape)
    manager_data=manager_data[~(manager_data["managername"].str.contains(" LLC",na=False))]
    manager_data=manager_data[~(manager_data["managername"].str.contains(" TRUST",na=False))]
    manager_data=manager_data[~(manager_data["managername"].str.contains(" MGMT",na=False))]
    manager_data=manager_data[~(manager_data["managername"].str.contains(" Team",na=False))]
    manager_data=manager_data[~(manager_data["managername"].str.contains(" INC",na=False))]
    manager_data=manager_data[~(manager_data["managername"]==1)]
    manager_data=manager_data[~manager_data["managername"].isna()]
    manager_data["managerfirstname"]=manager_data.apply(lambda x :get_name(x["managername"],"first"),axis=1)
    manager_data["managerlastname"]=manager_data.apply(lambda x :get_name(x["managername"],"last"),axis=1)
    manager_data["managermiddlename"]=manager_data.apply(lambda x :get_name(x["managername"],"middle"),axis=1)
    print("manager data shape",manager_data.shape)
    # manager_data.head()
    manager_data_notna=manager_data[~manager_data["managerlastname"].isna()]
    print("removing na manager data shape",manager_data_notna.shape)
    return manager_data_notna


# In[37]:


def get_split_on_and(s1,s2):
    s0=str(s1).split("&")[0]
    #add scenario of keepnig the name after & in seller name2
    return s0

def get_middle_name(sl,sf,s):
    total_str=s.split(" ")
    if len(total_str)>2:
        return total_str[2].strip()
    else:
        return ""
    
def get_middle_name_owner(sf):
    try:
        return sf.split(" ",1)[1]
    except:
        return ""

def get_first_name_del(fir,mid):
    firL=set(str(fir).split(" "))
    remFir=list(firL.difference(set(list(mid))))
    if remFir:
        return remFir[0]
    else:
        return fir


# In[38]:


def read_data(file_path):
    mortage_data=pd.read_stata(file_path)
#     mortage_data=pd.read_csv(file_path)
    mortage_data.reset_index(drop=True,inplace=True)
    print("shape of mortgage data",mortage_data.shape)
    return mortage_data


# In[39]:



def read_manager_data():
    manager_data=pd.read_excel("/scratch/user/rashmi_1996/LexisNexis_Managers.xlsx",sheet_name="data",engine="openpyxl")
    return manager_data


manager_data=read_manager_data()
manager_data_notna=preprocess_manager_data(manager_data)


# In[40]:


def get_seller_owner_info(mortage_data):
    seller_info=mortage_data[["record_number","sellerlastname","sellerfirstname","sellername1","sellername2"]]
    seller_info=seller_info[~seller_info["sellername1"].str.contains("COUNTY",na=False)]
    print("Getting only seller information - shape",seller_info.shape)

    seller_info=seller_info[~((seller_info["sellername1"].str.endswith(" LLC",na=False)) &
                             (seller_info["sellername2"].isna()))]
    seller_info=seller_info[~((seller_info["sellername1"].str.endswith(" TRUST ",na=False)) &
                             (seller_info["sellername2"].isna()))]
    seller_info=seller_info[~((seller_info["sellername1"].str.endswith(" MGMT",na=False)) &
                             (seller_info["sellername2"].isna()))]
    seller_info=seller_info[~((seller_info["sellername1"].str.endswith(" TRUST 2",na=False)) &
                             (seller_info["sellername2"].isna()))]
    seller_info=seller_info[~((seller_info["sellername1"].str.endswith(" INC",na=False)) &
                             (seller_info["sellername2"].isna()))]

    seller_info=seller_info[~((seller_info["sellername1"].str.endswith(" MTG",na=False)) &
                             (seller_info["sellername2"].isna()))]


    seller_info=seller_info[~((seller_info["sellername1"].str.endswith(" LOANS",na=False)) &
                             (seller_info["sellername2"].isna()))]

    seller_info=seller_info[~((seller_info["sellername2"].str.endswith(" LLC",na=False)) &
                             (seller_info["sellername1"].isna()))]
    seller_info=seller_info[~((seller_info["sellername2"].str.endswith(" TRUST ",na=False)) &
                             (seller_info["sellername1"].isna()))]
    seller_info=seller_info[~((seller_info["sellername2"].str.endswith(" MGMT",na=False)) &
                             (seller_info["sellername1"].isna()))]
    seller_info=seller_info[~((seller_info["sellername2"].str.endswith(" TRUST 2",na=False)) &
                             (seller_info["sellername1"].isna()))]

    seller_info=seller_info[~(seller_info["sellername1"].isna() & (seller_info["sellername2"].isna()))]


    seller_info=seller_info[seller_info["sellername1"]!=""]
    seller_info["sellername1"]=seller_info.apply(lambda x:get_split_on_and(x["sellername1"],x["sellername2"]),axis=1)
    seller_info["sellermidname"]=seller_info.apply(lambda x : get_middle_name(x["sellerlastname"],
                                                                              x["sellerfirstname"],
                                                                              x["sellername1"]),axis=1)

    print("complete seller info ",seller_info.shape)
    seller2_info=seller_info[~seller_info["sellername2"].isna()]
    print("seller 2 info ",seller2_info.shape)
    seller1_info=seller_info[["record_number","sellerlastname","sellerfirstname","sellername1","sellermidname"]]
    seller1_info=seller1_info[~(seller1_info["sellerlastname"].isna() & (seller1_info["sellerfirstname"].isna()))]
    print("seller 1 info ",seller1_info.shape)

    owner_info=mortage_data[["record_number","owner1lastname","owner1firstnamemi","owner2firstnamemi","owner2lastname"]]
    owner_info=owner_info[~((owner_info["owner1lastname"].str.contains("COUNTY",na=False)) &
                             (owner_info["owner2lastname"].isna()))]
    owner_info=owner_info[~((owner_info["owner1lastname"].str.contains(" LLC",na=False)) &
                             (owner_info["owner2lastname"].isna()))]
    owner_info=owner_info[~((owner_info["owner1lastname"].str.contains(" TRUST ",na=False)) &
                             (owner_info["owner2lastname"].isna()))]
    owner_info=owner_info[~((owner_info["owner1lastname"].str.contains(" MGMT",na=False)) &
                             (owner_info["owner2lastname"].isna()))]
    owner_info=owner_info[~((owner_info["owner1lastname"].str.contains(" TRUST 2",na=False)) &
                             (owner_info["owner2lastname"].isna()))]

    owner_info=owner_info[~((owner_info["owner2lastname"].str.contains("COUNTY",na=False)) &
                             (owner_info["owner1lastname"].isna()))]

    owner_info=owner_info[~((owner_info["owner2lastname"].str.contains(" MTG",na=False)) &
                             (owner_info["owner1lastname"].isna()))]


    owner_info=owner_info[~((owner_info["owner2lastname"].str.contains(" LOANS",na=False)) &
                             (owner_info["owner1lastname"].isna()))]

    owner_info=owner_info[~((owner_info["owner2lastname"].str.contains(" LLC",na=False)) &
                             (owner_info["owner1lastname"].isna()))]
    owner_info=owner_info[~((owner_info["owner2lastname"].str.contains(" TRUST ",na=False)) &
                             (owner_info["owner1lastname"].isna()))]
    owner_info=owner_info[~((owner_info["owner2lastname"].str.contains(" MGMT",na=False)) &
                             (owner_info["owner1lastname"].isna()))]
    owner_info=owner_info[~((owner_info["owner2lastname"].str.contains(" TRUST 2",na=False)) &
                             (owner_info["owner1lastname"].isna()))]

    owner_info=owner_info[~(owner_info["owner1lastname"].isna() & (owner_info["owner2lastname"].isna()) &
                             owner_info["owner1firstnamemi"].isna() & (owner_info["owner2firstnamemi"].isna()))]
    
    
    print("complete owner info ",owner_info.shape)
    owner1_info=owner_info[["record_number","owner1lastname","owner1firstnamemi"]]
    owner1_info=owner1_info[~(owner1_info["owner1lastname"].isna() & (owner1_info["owner1firstnamemi"].isna()))]
    owner1_info["owner1midname"]=owner_info.apply(lambda x : get_middle_name_owner(x["owner1firstnamemi"]),axis=1)

    owner1_info["owner1firstnamemi"]=owner1_info.apply(lambda x :get_first_name_del
                                                   (x["owner1firstnamemi"],x["owner1midname"]),axis=1)
    print("owner 1 info ",owner1_info.shape)
    owner2_info=owner_info[["record_number","owner2lastname","owner2firstnamemi"]]
    owner2_info=owner2_info[~(owner2_info["owner2lastname"].isna() & (owner2_info["owner2firstnamemi"].isna()))]
    owner2_info["owner2midname"]=owner_info.apply(lambda x : get_middle_name_owner(x["owner2firstnamemi"]),axis=1)
    owner2_info["owner2firstnamemi"]=owner2_info.apply(lambda x :get_first_name_del
                                               (x["owner2firstnamemi"],x["owner2midname"]),axis=1)
    print("owner 2 info ",owner2_info.shape)
    return seller1_info,seller2_info,owner1_info,owner2_info


# In[41]:


def get_first_lastname_seller_data(manager_data_notna,seller1_info):
    print("___________________sellername and manager data mapping______")
    seller_info_notna=seller1_info[~seller1_info["sellerlastname"].isna()]
    seller_info_notna=seller_info_notna[seller_info_notna["sellerlastname"]!=""]
    merge_data_lastname=pd.merge(manager_data_notna,seller_info_notna,left_on="managerlastname",right_on="sellerlastname")
    print("seller last name manager match ",merge_data_lastname["obs_manager"].nunique())
    seller_last_first=merge_data_lastname[merge_data_lastname["managerfirstname"]==merge_data_lastname["sellerfirstname"]]
    print("seller last and first name manager match",seller_last_first.obs_manager.nunique())
    seller_last_first=seller_last_first[seller_last_first["managermiddlename"]==seller_last_first["sellermidname"]]
    print("seller last,middle and first name manager match",seller_last_first.obs_manager.nunique())
#     print("seller last and first name record number match",seller_last_first.record_number.nunique())
    return seller_last_first

def get_first_lastname_owner_data(manager_data_notna,owner1_info):
    print("___________________owner1 and manager data mapping______")
    owner1_info_notna=owner1_info[~owner1_info["owner1lastname"].isna()]
    owner1_info_notna=owner1_info_notna[owner1_info_notna["owner1lastname"]!=""]
    merge_data_lastname=pd.merge(manager_data_notna,owner1_info_notna,left_on="managerlastname",right_on="owner1lastname")
    print("owner 1 last name manager match ",merge_data_lastname["obs_manager"].nunique())
    owner_last_first=merge_data_lastname[merge_data_lastname["managerfirstname"]==merge_data_lastname["owner1firstnamemi"]]
    print("owner last and first name manager match",owner_last_first.obs_manager.nunique())
    owner_last_first=owner_last_first[owner_last_first["managermiddlename"]==owner_last_first["owner1midname"]]
    print("owner last,middle and first name manager match",owner_last_first.obs_manager.nunique())
#     print("owner last and first name record number match",owner_last_first.record_number.nunique())
    return owner_last_first

def get_first_lastname_owner_data_v2(manager_data_notna,owner2_info):
    print("___________________owner2 and manager data mapping______")
    owner2_info_notna=owner2_info[~owner2_info["owner2lastname"].isna()]
    owner2_info_notna=owner2_info_notna[owner2_info_notna["owner2lastname"]!=""]
    merge_data_lastname=pd.merge(manager_data_notna,owner2_info_notna,left_on="managerlastname",right_on="owner2lastname")
    print("owner 2 last name manager match ",merge_data_lastname["obs_manager"].nunique())
    owner_last_first=merge_data_lastname[merge_data_lastname["managerfirstname"]==merge_data_lastname["owner2firstnamemi"]]
    print("owner 2 last and first name manager match",owner_last_first.obs_manager.nunique())
    owner_last_first=owner_last_first[owner_last_first["managermiddlename"]==owner_last_first["owner2midname"]]
    print("owner 2 last,middle and first name manager match",owner_last_first.obs_manager.nunique())
#     print("owner 2 last and first name record number match",owner_last_first.record_number.nunique())
    return owner_last_first

def get_final_data(manager_data_notna,mortage_data):
    seller1_info,seller2_info,owner1_info,owner2_info=get_seller_owner_info(mortage_data)
    seller_last_first_middle=get_first_lastname_seller_data(manager_data_notna,seller1_info)
    owner1_last_first_middle=get_first_lastname_owner_data(manager_data_notna,owner1_info)
    owner2_last_first_middle=get_first_lastname_owner_data_v2(manager_data_notna,owner2_info)
    seller_last_first_middle["sellerORowner"]="seller"
    owner1_last_first_middle["sellerORowner"]="owner1"
    owner2_last_first_middle["sellerORowner"]="owner2"
    seller_last_first_middle.rename(columns={'sellerfirstname':'first_name', 'sellermidname':'mid_name','sellerlastname':'last_name','sellername1':'total_name'}, inplace=True)
    owner1_last_first_middle.rename(columns={'owner1firstnamemi':'first_name', 'owner1midname':'mid_name','owner1lastname':'last_name'}, inplace=True)
    owner2_last_first_middle.rename(columns={'owner2firstnamemi':'first_name', 'owner2midname':'mid_name','owner2lastname':'last_name'}, inplace=True)
    owner1_last_first_middle["total_name"]=""
    owner2_last_first_middle["total_name"]=""
    total_data=pd.concat([seller_last_first_middle,owner1_last_first_middle,owner2_last_first_middle],ignore_index=True)
    total_data=total_data.sort_values(["obs_manager"]).drop_duplicates()
    print("number of obs manger final data unique",total_data.obs_manager.nunique())
    total_data=total_data.reset_index(drop=True)
    print("number of records mapped final data unique",total_data.record_number.nunique())
    return total_data


# In[ ]:


# "/scratch/user/rashmi_1996/Data_1/mortgage_deeds_1_addmasked_1.dta"
final_output_file=pd.DataFrame()
dir_path = r'/scratch/user/rashmi_1996/Data_1'
count=0
for path in os.listdir(dir_path):
    count+=1
    print("Running for next file",path,count)
    if os.path.isfile(os.path.join(dir_path, path)):
#         file_path="/scratch/user/rashmi_1996/Data_1/mortgage_deeds_12_addmasked_1.dta"
        file_path=os.path.join(dir_path, path)
        start_time1 = datetime.now()
        try:
            mortage_data=read_data(file_path)
            print("columns mortage_data",mortage_data.columns)
            mortage_data.drop(["valid_longitude","valid_latitude","valid_mailadd"],axis=1,inplace=True)
            mortage_data.drop_duplicates(['owner1lastname_cl', 'owner1firstnamemi_cl', 'owner2lastname',
           'owner2firstnamemi', 'sellerlastname', 'sellerfirstname', 'sellername1',
           'sellername2'],inplace=True)
            mortage_data.rename(columns={'owner1lastname_cl':'owner1lastname', 'owner1firstnamemi_cl':'owner1firstnamemi'}, inplace=True)
            print("checking complete direct match")
            print(pd.merge(manager_data_notna,mortage_data,left_on="managername",right_on="sellername1").shape)
    #         print(pd.merge(manager_data_notna,mortage_data,left_on="managername",right_on="sellername2").shape)
            total_data=get_final_data(manager_data_notna,mortage_data)
            total_data["file_name"]="_".join(re.findall(r'\d+', path))
            final_output_file=pd.concat([final_output_file,total_data])
            end_time1 = datetime.now()
            print('Duration for {} is {}'.format(path,end_time1 - start_time1))
            try:
                del mortage_data
            except:
                pass
            try:
                del manager_data
            except:
                pass
            try:
                del total_data
            except:
                pass
            gc.collect()
        except:
            print("*********",path)
            pass
        
final_output_file.to_csv(dir_path+"/final_output_file.csv")


# In[ ]:


# file_path="./mortgage_deeds_10.csv"
# mortage_data=read_data(file_path)
# print("checking complete direct match")
# print(pd.merge(manager_data_notna,mortage_data,left_on="managername",right_on="sellername1").shape)
# print(pd.merge(manager_data_notna,mortage_data,left_on="managername",right_on="sellername2").shape)
# total_data=get_final_data(manager_data_notna,mortage_data)


# In[ ]:


# a=read_data('/scratch/user/rashmi_1996/Data_1/mortgage_deeds_12_addmasked_1.dta')
# print(a.shape)
# b=read_data('/scratch/user/rashmi_1996/Data_1/mortgage_deeds_12_addmasked_2.dta')
# print(b.shape)
# a.drop(["record_number"],inplace=True,axis=1)
# b.drop(["record_number"],inplace=True,axis=1)
# a.drop_duplicates(inplace=True)
# b.drop_duplicates(inplace=True)
# print(a.shape)
# print(b.shape)
# c=pd.concat([a, b], axis=0)
# print(c.shape)
# d=c[c.duplicated()]
# print(d.shape)


# In[ ]:


end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))


# # In[ ]:


# import pandas as pd
# get_ipython().run_line_magic('whos', 'DataFrame')


# # In[ ]:


# import sys
# def sizeof_fmt(num, suffix='B'):
#     ''' by Fred Cirera,  https://stackoverflow.com/a/1094933/1870254, modified'''
#     for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
#         if abs(num) < 1024.0:
#             return "%3.1f %s%s" % (num, unit, suffix)
#         num /= 1024.0
#     return "%.1f %s%s" % (num, 'Yi', suffix)

# for name, size in sorted(((name, sys.getsizeof(value)) for name, value in locals().items()),
#                          key= lambda x: -x[1])[:10]:
#     print("{:>30}: {:>8}".format(name, sizeof_fmt(size)))


# In[ ]:



# seller_last_first_middle=seller_last_first[seller_last_first["managermiddlename"]==seller_last_first["sellermidname"]]
# print(seller_last_first_middle.obs_manager.nunique())
# print(seller_last_first_middle.record_number.nunique())
# print(seller_last_first.to_string())
# df=seller_last_first[["managername","sellername1"]].reset_index(drop=True).drop_duplicates()
# # display(df.to_string())
# with pd.option_context('display.max_rows', None,
#                        'display.max_columns', None,
#                        'display.precision', 3,
#                        ):
#     print(df)
# seller_last_first_middle

