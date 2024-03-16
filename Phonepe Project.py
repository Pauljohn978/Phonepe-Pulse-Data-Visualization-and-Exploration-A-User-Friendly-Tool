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
                Transanction_Amount bigint
            )
        """
cursor.execute(create_query_7)
mydb.commit()

insert_query_7 = """insert into top_insur (States, Years, Quarters, Pincodes, Transanction_Count, Transanction_Amount)
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
                Pincode bigint,
                Transanction_Count bigint,
                Transanction_Amount bigint
            )
        """
cursor.execute(create_query_8)
mydb.commit()

insert_query_8 = """insert into top_transanction (States, Years, Quarters, Pincode, Transanction_Count, Transanction_Amount)
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
                Pincode bigint,
                registeredUsers bigint
            )
        """
cursor.execute(create_query_9)
mydb.commit()

insert_query_9 = """insert into top_user (States, Years, Quarters, Pincode, registeredUsers)
                                                 values(%s, %s, %s, %s, %s)"""
data = top_user.values.tolist()
cursor.executemany(insert_query_9, data)
mydb.commit()
 

#aggre_insurance
cursor.execute("select * from aggre_insurance")

table1 = cursor.fetchall()

Aggre_insurance = pd.DataFrame(table1, columns = ("States", "Years", "Quarters", "Transanction Type", "Transanction Count", "Transanction Amount"))

cursor.execute("select * from aggre_transanction")

table2 = cursor.fetchall()

Aggre_transanction = pd.DataFrame(table2, columns = ("Years", "States", "Quarters", "Transanction Type", "Transanction Count", "Transanction Amount"))

#aggre_user
cursor.execute("select * from aggre_user")

table3 = cursor.fetchall()

Aggre_user = pd.DataFrame(table3, columns = ("States", "Years", "Quarter", "Brand", "Transanction Count", "Percentage"))

#map_insurance
cursor.execute("select * from map_insurance")

table4 = cursor.fetchall()

Map_insurance = pd.DataFrame(table4, columns = ("States", "Years", "Quarter", "District", "Transanction Count", "Transanction Amount"))

#map_transanction
cursor.execute("select * from map_trans")

table5 = cursor.fetchall()

Map_trans = pd.DataFrame(table5, columns = ("States", "Years", "Quarter", "District", "Transanction Count", "Transanction Amount"))

#map_user
cursor.execute("select * from map_user")

table6 = cursor.fetchall()

Map_user = pd.DataFrame(table6, columns = ("States", "Years", "Quarter", "District", "Registered Users", "App Opens"))

#top_insur
cursor.execute("select * from top_insur")

table7 = cursor.fetchall()

Top_insur = pd.DataFrame(table7, columns = ("States", "Years", "Quarter", "Pincode", "Transanction Count", "Transanction Amount"))

#top_transanction
cursor.execute("select * from top_transanction")

table8 = cursor.fetchall()

top_transanction = pd.DataFrame(table8, columns = ("States", "Years", "Quarter", "Pincode", "Transanction Count", "Transanction Amount"))

#top_user
cursor.execute("select * from top_user")

table9 = cursor.fetchall()

top_user = pd.DataFrame(table9, columns = ("States", "Years", "Quarter", "Pincode", "Registered Users"))


import plotly.express as px
import requests 
import json

def transanction_amount_count_Y(df, year, quarter):
    quarter =str(quarter)
    tacy = df[(df["Years"] == year) & (df["Quarters"] == quarter)]

    tacy["Years"].unique()
    tacy.reset_index(drop = True, inplace=True)

    tacyg = tacy.groupby("States")[["Transanction Count", "Transanction Amount"]].sum()
    tacyg.reset_index(inplace = True)

    col1, col2 = st.columns(2)
    with col1:
        fig_amount = px.bar(tacyg, x = "States", y = "Transanction Amount", title=f"Transanction Amount Bar Graph for the year {year} Q {quarter}")
        st.plotly_chart(fig_amount)
    with col2:
        fig_count = px.bar(tacyg, x = "States", y = "Transanction Count", title=f"Transanction Count Bar Graph for the year {year} Q {quarter}")
        st.plotly_chart(fig_count)
    
    col1, col2 = st.columns(2)
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    states_name = []
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])

    states_name.sort()

    with col1:
        fig_india_1 = px.choropleth(tacyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                    color="Transanction Amount", color_continuous_scale="Blues",
                                    range_color=(tacyg["Transanction Amount"].min(), tacyg["Transanction Amount"].max()),
                                    hover_name="States", title=f"Transaction Amount for {year} Q{quarter}", fitbounds="locations",
                                    height=600, width=600)
        fig_india_1.update_geos(visible = False)
        st.plotly_chart(fig_india_1)

    with col2:
        fig_india_2 = px.choropleth(tacyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                    color="Transanction Count", color_continuous_scale="Blues",
                                    range_color=(tacyg["Transanction Count"].min(), tacyg["Transanction Count"].max()),
                                    hover_name="States", title=f"Transanction Count for {year} Q{quarter}", fitbounds="locations",
                                    height=600, width=600)
        fig_india_2.update_geos(visible = False)
        st.plotly_chart(fig_india_2)

def Agg_transanc(df, state):
        atsw = df[df["States"]==state]
        atsw.reset_index(drop = True, inplace= True)
        atswg = atsw.groupby("Transanction Type")[["Transanction Count", "Transanction Amount"]].sum()
        atswg.reset_index(inplace = True)
        fig_pie_1 = px.pie(data_frame=atswg, names = "Transanction Type", values="Transanction Amount", width=600, title=f"Transanction Amount for {state}",
                      hole = 0.0)
        st.plotly_chart(fig_pie_1)
        fig_pie_2 = px.pie(data_frame=atswg, names = "Transanction Type", values="Transanction Count", width=600, title=f"Transanction Count for {state}",
                      hole = 0.0)
        st.plotly_chart(fig_pie_2)

def Aggre_user_plot1(df, year, quarter):
    year = str(year)
    aguy = df[(df["Years"]== year)& (df["Quarter"] == quarter)]
    aguy.reset_index(drop=True, inplace = True)
    aguyg = pd.DataFrame(aguy.groupby("Brand")["Transanction Count"].sum())
    aguyg.reset_index(inplace=True)
    aguyg

    fig_bar_1 = px.bar(aguyg, x = "Brand", y = "Transanction Count", title = f"Brand & Transanction Count for {year} {quarter}", width=600)
    st.plotly_chart(fig_bar_1)

def Map_insur_District(df, state):
    tacy = df[(df["States"] == state)]
    tacy.reset_index(drop = True, inplace=True)

    tacyg = tacy.groupby("District")[["Transanction Count", "Transanction Amount"]].sum()
    tacyg.reset_index(inplace = True)
    fig_amount = px.bar(tacyg, y = "Transanction Count", x = "District", title=f"Transanction Amount Bar Graph for {state}")
    st.plotly_chart(fig_amount)
    fig_count = px.bar(tacyg, y = "Transanction Amount", x = "District", title=f"Transanction Count Bar Graph for {state}")
    st.plotly_chart(fig_count)

def Map_insur_App(df, state, year, quarter):
    tacy = df[(df["States"] == state) & (df["Years"] == year) & (df["Quarter"] == quarter)]
    tacy.reset_index(drop = True, inplace=True)

    tacyg = tacy.groupby("District")[["Registered Users", "App Opens"]].sum()
    tacyg.reset_index(inplace = True)

    fig_pie_1 = px.pie(data_frame=tacyg, names = "District", values="Registered Users", width=600, title=f"Registered Users for {state}",
                      hole = 0.0)
    st.plotly_chart(fig_pie_1)
    fig_pie_2 = px.pie(data_frame=tacyg, names = "District", values="App Opens", width=600, title=f"App Opens for {state}",
                      hole = 0.0)
    st.plotly_chart(fig_pie_2)

def top_trans_list(year, df, quarter):
    Top_states_list = Top_insur[(Top_insur["Years"]==year) & (Top_insur["Quarter"]==quarter)].groupby('States')['Transanction Amount'].sum().sort_values(ascending=False).head(10)
    Top_states_list1 = pd.DataFrame(Top_states_list)

    Top_pincode_list = Top_insur[(Top_insur["Years"]==year) & (Top_insur["Quarter"]==quarter)].groupby('Pincode')['Transanction Amount'].sum().sort_values(ascending=False).head(10)
    Top_pincode_list1 = pd.DataFrame(Top_pincode_list)

    st.write("Top 10 States by Transanction Amount")
    st.write(Top_states_list1)
    st.write("Top 10 Pincodes by Transanction Amount")
    st.write(Top_pincode_list1)

def top_user_list(df, year, quarter):
    top_user_state = top_user[(top_user["Years"]==year) & (top_user["Quarter"]==quarter)].groupby("States")["Registered Users"].sum().sort_values(ascending=False).head(10)
    top_user_pin = top_user[(top_user["Years"]==year) & (top_user["Quarter"]==quarter)].groupby("Pincode")["Registered Users"].sum().sort_values(ascending=False).head(10)
    top_user_state1 = pd.DataFrame(top_user_state)
    top_user_pin1 = pd.DataFrame(top_user_pin)
    st.write("Top 10 States by Register Users")
    st.write(top_user_state1)
    st.write("Top 10 Pincodes by Register Users")
    st.write(top_user_pin1)


#Streamlit
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import psycopg2

#streamlit app

st.set_page_config("Phonepe Project")
sidebar_name = st.sidebar.selectbox("Select an Option", ["Aggregation Analysis", "Map Analysis", "Top Analysis"])

if sidebar_name=="Aggregation Analysis":
    analysis1 = st.selectbox("Select a Data Set for Exploring Data:",("Select here:","Insurance Analysis", "Transanction Analysis", "User Analysis"))
    if analysis1 == "Insurance Analysis":
            st.write("Pressed Insurance Aggregation Analysis")
            year = st.selectbox("Select Year", ["2020", "2021", "2022", "2023"])
            quarter = st.slider("Select Quarter", 1, 4)
            transanction_amount_count_Y(Aggre_insurance, year, quarter)
        
    elif analysis1 == "Transanction Analysis":
            st.write("Pressed Transanction Transanction Analysis")
            state = st.selectbox("Select state", ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                    'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                    'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                    'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                    'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                    'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                    'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                    'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                    'Uttarakhand', 'West Bengal'])
            df = Aggre_transanction
            Agg_transanc(df, state)
        
    elif analysis1 == "User Analysis":
        st.write("Pressed Transanction Transanction Analysis")
        year = st.selectbox("Select Year", ["2020", "2021", "2022"])
        quarter = st.slider("Select Quarter", 1, 4)
        df = Aggre_user
        Aggre_user_plot1(df, year, quarter)


if sidebar_name=="Map Analysis":
    analysis2 = st.selectbox("Select a Data Set for Map Analysis:",("Select here:","Insurance Analysis", "Transanction Analysis", "User Analysis"))
    
    if analysis2 == "Insurance Analysis":
            st.write("Pressed Map Insurance Analysis")
            year = st.selectbox("Select Year", ["2020", "2021", "2022", "2023"])
            df = Map_insurance
            state = st.selectbox("Select state", ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                    'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                    'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                    'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                    'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                    'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                    'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                    'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                    'Uttarakhand', 'West Bengal'])
            Map_insur_District(df, state)
    
    if analysis2 == "Transanction Analysis":
        st.write("Pressed Map Transanction Analysis")
        year = st.selectbox("Select Year", ["2020", "2021", "2022", "2023"])
        df = Map_trans
        state = st.selectbox("Select state", ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                    'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                    'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                    'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                    'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                    'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                    'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                    'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                    'Uttarakhand', 'West Bengal'])
        Map_insur_District(df, state)

    if analysis2 == "User Analysis":
        st.write("Pressed Map User Analysis")
        year = st.selectbox("Select Year", ["2020", "2021", "2022", "2023"])
        df = Map_user
        quarter = st.slider("Select Quarter", 1, 4)
        state = st.selectbox("Select state", ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                    'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                    'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                    'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                    'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                    'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                    'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                    'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                    'Uttarakhand', 'West Bengal'])
        Map_insur_App(df, state, year, quarter)


if sidebar_name=="Top Analysis":
    analysis3 = st.selectbox("Select a Data Set for Map Analysis:",("Select here:","Insurance Analysis", "Transanction Analysis", "User Analysis"))
    
    if analysis3=="Insurance Analysis":
        st.write("Pressed Top Insurance Analysis")
        year = st.selectbox("Select Year", ["2020", "2021", "2022", "2023"])
        df = Top_insur
        quarter = st.slider("Select Quarter", 1, 4)
        top_trans_list(year, df, quarter)

    if analysis3=="Transanction Analysis":
        st.write("Pressed Top Transanction Analysis")
        year = st.selectbox("Select Year", ["2020", "2021", "2022", "2023"])
        df = Top_insur
        quarter = st.slider("Select Quarter", 1, 4)
        top_trans_list(year, df, quarter)

    if analysis3=="User Analysis":
        st.write("Pressed Top User Analysis")
        year = st.selectbox("Select Year", ["2018","2019","2020", "2021", "2022", "2023"])
        df = top_user
        quarter = st.slider("Select Quarter", 1, 4)
        top_user_list(df, year, quarter)


# In[ ]:





# In[ ]:





# In[ ]:




