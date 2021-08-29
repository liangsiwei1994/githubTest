#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 16:23:46 2021

@author: siweiliang
"""

#PART 1 OF 4
#(RUN FROM PART 1 TO PREVENT ANY ERROR DUE TO DATABASE CREATION)
#REMEMBER TO CLEAR DATABASE AFTER RUNNING ALL SEQUENCE

import mysql.connector, sys, pandas as pd
from datetime import datetime
from bokeh.plotting import figure, show





#Importing Data from Excel
#Import Bitcoin Data
df = pd.read_csv("Crypto Data/Bitcoin Historical Data.csv", sep = ',', thousands=',', na_values = '-')
#Convert Volume to Integer
df['Vol.'] = df['Vol.'].str.replace('.', '')
df['Vol.'] = df['Vol.'].str.replace('K', '0')
df['Vol.'] = df['Vol.'].str.replace('M', '0000')
df['Vol.'] = df['Vol.'].str.replace('B', '000000000')
df['Vol.'] = df['Vol.'].str.replace('B', '000000000')
df = df.dropna()
df['Vol.'] = df['Vol.'].astype(int)
#Change string of Vol to int
df['Change %'] = df['Change %'].str.replace('%', '')
df['Change %'] = df['Change %'].astype(float)
print("*** Bitcoin Data ***")
print(df)


#Import DJI Data
df2 = pd.read_csv("data/DJIHistoricalPrices.csv", sep = ',')
print("*** Dow Jones Industrial Average Data ***")
print(df2)

#Import gold price
df3 = pd.read_csv("data/goldprices.csv", sep = ',')
df3 = df3.fillna(method = 'pad')
print("*** Gold Prices Data ***")
print(df3)





#Connecting to MySQL
user, pw, host, db = 'root','rootroot','127.0.0.1','sys'
cnx = mysql.connector.connect(user=user, password=pw, host=host, database=db)
cursor = cnx.cursor()





#Create database
query_for_creating_database = 'CREATE DATABASE CA2database'
try:
    cursor.execute(query_for_creating_database)
    cnx.commit()
    print("Database created!")
except:
    print("Unexpected error:", sys.exc_info()[0])
finally:
  cursor.close()
  cnx.close()





#Reconnect to new database
user, pw, host, db = 'root','rootroot','127.0.0.1','CA2database'
cnx = mysql.connector.connect(user=user, password=pw, host=host, database=db)
cursor = cnx.cursor()




  
#Create table
query_for_creating_table = ("CREATE TABLE `BTCData` ("
    "`Date` varchar(255) NOT NULL,"
    "`Price` double NOT NULL,"
    "`Open` double NOT NULL,"
    "`High` double NOT NULL,"
    "`Low` double NOT NULL,"
    "`Vol` varchar(100) NOT NULL,"
    "`Changes` varchar(100) NOT NULL,"
    "PRIMARY KEY (`Date`)"
    ") ENGINE=InnoDB")
try:
    cursor.execute(query_for_creating_table)
    cnx.commit()
    print("Table created!")

except:
    print("Unexpected error:", sys.exc_info()[0])
   

    


#Writing from csv to database
try:
    for index, col in df.iterrows():
        #rowdate = parse(col[0]).strftime("%b %d, %Y")
        dt_obj_rowdate = datetime.strptime(col[0], "%b %d, %Y").date()
        data = {
            'Date': dt_obj_rowdate,
            'Price': col[1],
            'Open': col[2],
            'High': col[3],
            'Low' : col[4],
            'Vol': col[5],
            'Changes': col[6],
          }
        query = insert_stmt = ("INSERT INTO BTCData (Date, Price, Open, High, Low, Vol, Changes)"
                                "VALUES (%(Date)s, %(Price)s, %(Open)s, %(High)s, %(Low)s, %(Vol)s, %(Changes)s)")
        cursor.execute(query, data)
        cnx.commit()
    print("All data inserted!")

except:
    print("Unexpected error:", sys.exc_info()[0])




#Extract records########
select_stmt = ("SELECT Date, Price FROM BTCData WHERE Date > %(Date)s")
data = { 'Date': '2017-01-01'} # (datetime(2012, 3, 23).date())
try:
  cursor.execute(select_stmt, data)
  extracted_SQLData = pd.DataFrame(cursor.fetchall(), 
                    columns = ['Date','Price'])
  extracted_SQLData['Date'] = pd.to_datetime(extracted_SQLData['Date'])
  print("Query finished!")
except:
    print("Unexpected error:", sys.exc_info()[0])
finally:
    cursor.close()
    cnx.close()
 
   
 
    
#Keep things at 2019 and onwards
extracted_SQLData = extracted_SQLData[extracted_SQLData['Date'] > '2019-01-01']
df3 = df3[df3['Date'] > '2019-01-01']
print("Data Filtered!")




#Convert date column of DJIA and Gold Price to datetime formate
df2["Date"] = df2["Date"].apply(pd.to_datetime)
df2['Date'] = pd.to_datetime(df2['Date']).dt.date
df2 = df2.sort_values(by='Date')

df3["Date"] = df3["Date"].apply(pd.to_datetime)
df3['Date'] = pd.to_datetime(df3['Date']).dt.date
print("Date Converted!")




#Print Bitcoin Graph
print("Drawing Graphs...")
p = figure(title="Price of Bitcoin, Dow Jones Industrial Average and Gold Across Time Since 2017", x_axis_label='Date', y_axis_label='Price in USD', width=2400, height=750, x_axis_type="datetime")
p.line(extracted_SQLData['Date'], extracted_SQLData['Price'], legend_label='Bitcoin')
p.line(df2['Date'], df2[' Close'], legend_label='Dow Jones Industrial Average', color = 'red')
p.line(df3['Date'], df3['Close'], legend_label='Gold', color = 'black')
p.legend.click_policy = "hide"
p.legend.location = 'top_left'
show(p)
print("Done!")