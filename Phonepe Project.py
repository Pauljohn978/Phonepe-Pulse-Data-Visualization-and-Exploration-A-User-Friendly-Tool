#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os 
import json
import pandas as pd

#Aggre_insurancehere
path1 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/aggregated/insurance/country/india/state/"
agg_insur_list = os.listdir(path1)

columns1 = {"States":[], "Years":[], "Quarter":[], "Transanction_type":[], "Transanction_count":[], "Transanction_amount":[]}

for state in agg_insur_list :
    cur_states = path1 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            A = json.load(data)
            
            for i in A["data"]['transactionData']:
                name = i["name"]
                count = i['paymentInstruments'][0]["count"]
                amount = i['paymentInstruments'][0]["amount"]
                columns1["Transanction_type"].append(name)
                columns1["Transanction_count"].append(count)
                columns1["Transanction_amount"].append(amount)    
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))
aggre_insurance = pd.DataFrame(columns1)
aggre_insurance["States"] = aggre_insurance["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
aggre_insurance["States"] = aggre_insurance["States"].str.replace('-', ' ')
aggre_insurance["States"] = aggre_insurance["States"].str.title()
aggre_insurance["States"] = aggre_insurance["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")

#Aggre_transanction

path2 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list = os.listdir(path2)

columns2 = {"States":[], "Years":[], "Quarter":[], "Transanction_type":[], "Transanction_count":[], "Transanction_amount":[]}

for state in agg_tran_list:
    cur_states = path2 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            B = json.load(data)
            
            for i in B["data"]['transactionData']:
                name = i["name"]
                count = i['paymentInstruments'][0]["count"]
                amount = i['paymentInstruments'][0]["amount"]
                columns2["Transanction_type"].append(name)
                columns2["Transanction_count"].append(count)
                columns2["Transanction_amount"].append(amount)
                columns2["States"].append(state)
                columns2["Years"].append(year)
                columns2["Quarter"].append(int(file.strip(".json")))

aggre_transanction=pd.DataFrame(columns2)
aggre_transanction["States"] = aggre_transanction["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
aggre_transanction["States"] = aggre_transanction["States"].str.replace('-', ' ')
aggre_transanction["States"] = aggre_transanction["States"].str.title()
aggre_transanction["States"] = aggre_transanction["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")

       
#aggre_user

path3 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/aggregated/user/country/india/state/"
    
columns3 = {"States":[], "Years":[], "Quarter":[], "Brands":[], "Transanction_count":[], "Percentage":[]}
agg_user_list = os.listdir(path3)

for state in agg_tran_list:
    cur_states = path3 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            C = json.load(data)
            
            try:
                for i in C["data"]['usersByDevice']:
                    brand = i["brand"]
                    count = i['count']
                    percentage = i['percentage']
                    columns3["Brands"].append(brand)
                    columns3["Transanction_count"].append(count)
                    columns3["Percentage"].append(percentage)
                    columns3["States"].append(state)
                    columns3["Years"].append(year)
                    columns3["Quarter"].append(int(file.strip(".json")))
                                               
            except:
                pass

aggre_user=pd.DataFrame(columns3)
aggre_user["States"] = aggre_user["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace('-', ' ')
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user["States"] = aggre_user["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")
                         
#map_insurance
path4 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/map/insurance/hover/country/india/state/"
map_insur_list = os.listdir(path4)

columns4 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transanction_count":[], "Transanction_amount":[]}
map_tran_list = os.listdir(path4)

for state in map_insur_list:
    cur_states = path4 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            D = json.load(data)
            try:
                for i in D["data"]["hoverDataList"]:
                    name = i["name"]
                    metric = i['metric'][0]["count"]
                    amount = i["metric"][0]["amount"]
                    columns4["Districts"].append(name)
                    columns4["Transanction_count"].append(count)
                    columns4["Transanction_amount"].append(amount)
                    columns4["States"].append(state)
                    columns4["Years"].append(year)
                    columns4["Quarter"].append(int(file.strip(".json")))
                                        
            except:
                pass
map_insurance = pd.DataFrame(columns4)
map_insurance["States"] = map_insurance["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
map_insurance["States"] = map_insurance["States"].str.replace('-', ' ')
map_insurance["States"] = map_insurance["States"].str.title()
map_insurance["States"] = map_insurance["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")  

#map trans
path5 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/map/transaction/hover/country/india/state/"

columns5 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transanction_count":[], "Transanction_amount":[]}
map_tran_list = os.listdir(path5)

for state in map_tran_list:
    cur_states = path5 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            E = json.load(data)
            try:
                for i in E["data"]["hoverDataList"]:
                    name = i["name"]
                    metric = i['metric'][0]["count"]
                    amount = i["metric"][0]["amount"]
                    columns5["Districts"].append(name)
                    columns5["Transanction_count"].append(count)
                    columns5["Transanction_amount"].append(amount)
                    columns5["States"].append(state)
                    columns5["Years"].append(year)
                    columns5["Quarter"].append(int(file.strip(".json")))
                                            
            except:
                pass

map_trans = pd.DataFrame(columns5)
map_trans["States"] = map_trans["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
map_trans["States"] = map_trans["States"].str.replace('-', ' ')
map_trans["States"] = map_trans["States"].str.title()
map_trans["States"] = map_trans["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")     
                         
#map_userHere
path6 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path6)
columns6 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "registeredUsers":[], "AppOpens":[]}

for state in map_user_list:
    cur_states = path6 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            F = json.load(data)
            try:
                for i in F["data"]["hoverData"].items():
                    district = i[0]
                    registeredUsers = i[1]["registeredUsers"]
                    appOpens = i[1]["appOpens"]
                    columns6["Districts"].append(district)
                    columns6["registeredUsers"].append(registeredUsers)
                    columns6["AppOpens"].append(appOpens)
                    columns6["States"].append(state)
                    columns6["Years"].append(year)
                    columns6["Quarter"].append(int(file.strip(".json")))
                                               
            except:
                pass

map_user = pd.DataFrame(columns6)
map_user["States"] = map_user["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace('-', ' ')
map_user["States"] = map_user["States"].str.title()
map_user["States"] = map_user["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")   
                    
#top_insurance
path7 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/top/insurance/country/india/state/"
top_insur_list = os.listdir(path7)
columns7 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transanction_Count":[], "Amount_Count":[]}

for state in top_insur_list:
    cur_states = path7 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            G = json.load(data)
            try:
                for i in G["data"]["pincodes"]:
                    entityname = i["entityName"]
                    count = i["metric"]["count"]
                    amount = i["metric"]["amount"]
                    columns7["Pincodes"].append(entityname)
                    columns7["Transanction_Count"].append(count)
                    columns7["Amount_Count"].append(amount)
                    columns7["States"].append(state)
                    columns7["Years"].append(year)
                    columns7["Quarter"].append(int(file.strip(".json")))
                                               
            except:
                pass

top_insur = pd.DataFrame(columns7)
top_insur["States"] = top_insur["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
top_insur["States"] = top_insur["States"].str.replace('-', ' ')
top_insur["States"] = top_insur["States"].str.title()
top_insur["States"] = top_insur["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")       
                
#top_transanction
path8 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/top/transaction/country/india/state/"
top_tran_list = os.listdir(path8)
columns8 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transanction_Count":[], "Amount_Count":[]}

for state in top_tran_list:
    cur_states = path8 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            H = json.load(data)
            try:
                for i in H["data"]["pincodes"]:
                    entityname = i["entityName"]
                    count = i["metric"]["count"]
                    amount = i["metric"]["amount"]
                    columns8["Pincodes"].append(entityname)
                    columns8["Transanction_Count"].append(count)
                    columns8["Amount_Count"].append(amount)
                    columns8["States"].append(state)
                    columns8["Years"].append(year)
                    columns8["Quarter"].append(int(file.strip(".json")))
                                               
            except:
                pass
                       
top_transanction = pd.DataFrame(columns8)
top_transanction["States"] = top_transanction["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
top_transanction["States"] = top_transanction["States"].str.replace('-', ' ')
top_transanction["States"] = top_transanction["States"].str.title()
top_transanction["States"] = top_transanction["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")     
 
#top_user
path9 = "C:/Users/finny/OneDrive/Desktop/Git/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path9)
columns9 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "registeredUsers":[]}

for state in top_user_list:
    cur_states = path9 + state + "/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year = cur_states + year + "/"
        agg_file_list = os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, "r")
    
            I = json.load(data)
            try:
                for i in I["data"]["pincodes"]:
                    entityname = i["name"]
                    registeredUsers = i["registeredUsers"]
                    columns9["Pincodes"].append(entityname)
                    columns9["registeredUsers"].append(registeredUsers)
                    columns9["States"].append(state)
                    columns9["Years"].append(year)
                    columns9["Quarter"].append(int(file.strip(".json")))
                                               
            except:
                pass
                       
top_user = pd.DataFrame(columns9)
top_user["States"] = top_user["States"].str.replace('andaman-&-nicobar-islands', "Andaman & Nicobar")
top_user["States"] = top_user["States"].str.replace('-', ' ')
top_user["States"] = top_user["States"].str.title()
top_user["States"] = top_user["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', "Dadra and Nagar Haveli and Daman and Diu")                                                                                            

#Connection to MySQL
import mysql.connector
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="MySQL", 
    database="phonepe",
    port=3306
)
cursor=mydb.cursor()

#aggregated_insurance_table
create_query_1 = """
            CREATE TABLE IF NOT EXISTS aggre_insurance (
                States varchar(255),
                Years VARCHAR(255),
                Quarters VARCHAR(255),
                Transanction_type VARCHAR(255),
                Transanction_count bigint,
                Transanction_amount bigint
            )
        """
cursor.execute(create_query_1)
mydb.commit()

insert_query_1 = """insert into aggre_insurance (States, Years, Quarters, Transanction_type, Transanction_count, Transanction_amount)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = aggre_insurance.values.tolist()
cursor.executemany(insert_query_1, data)
mydb.commit()

#aggregated_transanction_table
create_query_2 = """
            CREATE TABLE IF NOT EXISTS aggre_transanction(
                Years VARCHAR(255),
                States VARCHAR(255),
                Quarters VARCHAR(255),
                Transanction_type VARCHAR(255),
                Transanction_count bigint,
                Transanction_amount bigint
            )
        """
cursor.execute(create_query_2)
mydb.commit()

insert_query_2 = """insert into aggre_transanction (States, Years, Quarters, Transanction_type, Transanction_count, Transanction_amount)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = aggre_transanction.values.tolist()
cursor.executemany(insert_query_2, data)
mydb.commit()
                         
#aggregated_user_table
create_query_3 = """
            CREATE TABLE IF NOT EXISTS aggre_user(
                States VARCHAR(255),
                Years VARCHAR(255),
                Quarters int,
                Brands VARCHAR(255),
                Transanction_count bigint,
                percentage bigint
            )
        """
cursor.execute(create_query_3)
mydb.commit()

insert_query_3 = """insert into aggre_user (States, Years, Quarters, Brands, Transanction_count, Percentage)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = aggre_user.values.tolist()
cursor.executemany(insert_query_3, data)
mydb.commit()
                         
#map_insurance_table
create_query_4 = """
            CREATE TABLE IF NOT EXISTS map_insurance(
                States VARCHAR(255),
                Years VARCHAR(255),
                Quarters int,
                Districts VARCHAR(255),
                Transanction_count bigint,
                Transanction_amount bigint
            )
        """
cursor.execute(create_query_4)
mydb.commit()

insert_query_4 = """insert into map_insurance (States, Years, Quarters, Districts, Transanction_count, Transanction_amount)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = map_insurance.values.tolist()
cursor.executemany(insert_query_4, data)
mydb.commit()
                         
#map_transanction_table
create_query_5 = """
            CREATE TABLE IF NOT EXISTS map_trans(
                States VARCHAR(255),
                Years VARCHAR(255),
                Quarters int,
                Districts VARCHAR(255),
                Transanction_count bigint,
                Transanction_amount bigint
            )
        """
cursor.execute(create_query_5)
mydb.commit()

insert_query_5 = """insert into map_trans (States, Years, Quarters, Districts, Transanction_count, Transanction_amount)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = map_trans.values.tolist()
cursor.executemany(insert_query_5, data)
mydb.commit()
                         
#map_user_table
create_query_6 = """
            CREATE TABLE IF NOT EXISTS map_user(
                States VARCHAR(255),
                Years VARCHAR(255),
                Quarters int,
                Districts VARCHAR(255),
                registeredUsers bigint,
                AppOpens bigint
            )
        """
cursor.execute(create_query_6)
mydb.commit()

insert_query_6 = """insert into map_user (States, Years, Quarters, Districts, registeredUsers, AppOpens)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = map_user.values.tolist()
cursor.executemany(insert_query_6, data)
mydb.commit()
                         
#top_insurance_table
create_query_7 = """
            CREATE TABLE IF NOT EXISTS top_insur(
                States VARCHAR(255),
                Years VARCHAR(255),
                Quarters int,
                Pincodes bigint,
                Transanction_Count bigint,
                Amount_Count bigint
            )
        """
cursor.execute(create_query_7)
mydb.commit()

insert_query_7 = """insert into top_insur (States, Years, Quarters, Pincodes, Transanction_Count, Amount_Count)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = top_insur.values.tolist()
cursor.executemany(insert_query_7, data)
mydb.commit()
                         
#top_transanction_table
create_query_8 = """
            CREATE TABLE IF NOT EXISTS top_transanction(
                States VARCHAR(255),
                Years VARCHAR(255),
                Quarters int,
                Pincodes bigint,
                Transanction_Count bigint,
                Amount_Count bigint
            )
        """
cursor.execute(create_query_8)
mydb.commit()

insert_query_8 = """insert into top_transanction (States, Years, Quarters, Pincodes, Transanction_Count, Amount_Count)
                                                 values(%s, %s, %s, %s, %s, %s)"""
data = top_transanction.values.tolist()
cursor.executemany(insert_query_8, data)
mydb.commit()
                         
#top_user_table
create_query_9 = """
            CREATE TABLE IF NOT EXISTS top_user(
                States VARCHAR(255),
                Years VARCHAR(255),
                Quarters int,
                Pincodes bigint,
                registeredUsers bigint
            )
        """
cursor.execute(create_query_9)
mydb.commit()

insert_query_9 = """insert into top_user (States, Years, Quarters, Pincodes, registeredUsers)
                                                 values(%s, %s, %s, %s, %s)"""
data = top_user.values.tolist()
cursor.executemany(insert_query_9, data)
mydb.commit()
                         


# In[ ]:




