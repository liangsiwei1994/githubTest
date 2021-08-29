#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 16 00:47:49 2021

@author: siweiliang
"""

#PART 2 OF 4
#(RUN FROM PART 1 TO PREVENT ANY ERROR DUE TO DATABASE CREATION)
#REMEMBER TO CLEAR DATABASE AFTER RUNNING ALL SEQUENCE

import matplotlib.pyplot as plt
import mysql.connector, sys, pandas as pd
import datetime





#Connect to database
user, pw, host, db = 'root','rootroot','127.0.0.1','CA2database'
cnx = mysql.connector.connect(user=user, password=pw, host=host, database=db)
cursor = cnx.cursor()





#Extract relevant bitcoin records from database created in part 1
select_stmt = ("SELECT Date, Changes FROM BTCData ORDER BY Date DESC")
try:
  cursor.execute(select_stmt)
  extracted_SQLData = pd.DataFrame(cursor.fetchall(), 
                    columns = ['Date', 'Changes'])
  extracted_SQLData['Date'] = pd.to_datetime(extracted_SQLData['Date'])
  print("Query finished!")
except:
    print("Unexpected error:", sys.exc_info()[0])
finally:
    cursor.close()
    cnx.close()
    
extracted_SQLData['Changes'] = extracted_SQLData['Changes'].astype(float)
print("*** Bitcoin Data ***")
print(extracted_SQLData)
print()

#Import DJI Data
df2 = pd.read_csv("data/DJIHistoricalPrices.csv", sep = ',')
df2["Date"] = df2["Date"].apply(pd.to_datetime)
df2['Date'] = pd.to_datetime(df2['Date']).dt.date
df2 = df2.sort_values(by='Date')
print("*** Dow Jones Industrial Average Data ***")
print(df2)
print()

#Import gold price
df3 = pd.read_csv("data/goldprices.csv", sep = ',')
df3 = df3.fillna(method = 'pad')
df3["Date"] = df3["Date"].apply(pd.to_datetime)
df3['Date'] = pd.to_datetime(df3['Date']).dt.date
df3 = df3.sort_values(by='Date')
print("*** Gold Prices Data ***")
print(df3)
print()




#Calculate how much will USD$10,000 grow to from 02 Jan 2019 to 01 Jul 2021 if invest in Bitcoin
BTCData = extracted_SQLData[(extracted_SQLData['Date'] > '2018-12-31') & (extracted_SQLData['Date'] < '2021-07-01')]
BTCChange_Perc = BTCData['Changes'].sum()
BTCFinalAmount = 10000*BTCChange_Perc/100
print("How much will USD$10,000 become from 02 Jan 2019 to 01 Jul 2021 if invest in BTC:")
print(BTCFinalAmount)





#Calculate how much will USD$10,000 grow to from 02 Jan 2019 to 01 Jul 2021 if invest in DJIA
DJIAData_Start = df2[(df2['Date'] == datetime.date(2019,1,2))]
DJIAData_End = df2[(df2['Date'] == datetime.date(2021,7,1))]
DJIAFinalAmount = DJIAData_End[' Close'].item() / DJIAData_Start[' Close'].item() * 10000
print("How much will USD$10,000 grow to from 02 Jan 2019 to 01 Jul 2021 if invest in DJIA:")
print(DJIAFinalAmount)





#Calculate how much will USD$10,000 grow to from 02 Jan 2019 to 01 Jul 2021 if invest in Gold
GoldData_Start = df3[(df3['Date'] == datetime.date(2019,1,2))]
GoldData_End = df3[(df3['Date'] == datetime.date(2021,7,1))]
GoldFinalAmount = GoldData_End['Close'].item() / GoldData_Start['Close'].item() * 10000
print("How much will USD$10,000 grow to from 02 Jan 2019 to 01 Jul 2021 if invest in Gold:")
print(GoldFinalAmount)





#Plot Piechart
labels = ['Bitcoin', 'DJIA', 'Gold']
sizes = [BTCFinalAmount, DJIAFinalAmount, GoldFinalAmount]
explode = (0.1, 0, 0)
fig1, ax1 = plt.subplots(figsize = (18,12))
ax1.pie(sizes, explode = explode, labels = labels, shadow = True, autopct = '%1.1f%%', startangle = 90)
ax1.axis('equal')
ax1.set_title('Asset Distribution if invest equally at the start (From 02 Jan 2019 to 01 Jul 2021)')
plt.show()