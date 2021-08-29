#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 15 22:47:15 2021

@author: siweiliang
"""

#PART 4 OF 4
#(RUN FROM PART 1 TO PREVENT ANY ERROR DUE TO DATABASE CREATION)
#REMEMBER TO CLEAR DATABASE AFTER RUNNING ALL SEQUENCE

import matplotlib.pyplot as plt
import mysql.connector, sys, pandas as pd
import numpy as np




#Connect to database
user, pw, host, db = 'root','rootroot','127.0.0.1','CA2database'
cnx = mysql.connector.connect(user=user, password=pw, host=host, database=db)
cursor = cnx.cursor()





#Extract relevant bitcoin records from database created in part 1
select_stmt = ("SELECT Date, Price, Vol, Changes FROM BTCData ORDER BY Date DESC")
try:
  cursor.execute(select_stmt)
  extracted_SQLData = pd.DataFrame(cursor.fetchall(), 
                    columns = ['Date', 'Price','Vol', 'Changes'])
  extracted_SQLData['Date'] = pd.to_datetime(extracted_SQLData['Date'])
  print("Query finished!")
except:
    print("Unexpected error:", sys.exc_info()[0])
finally:
    cursor.close()
    cnx.close()
    
extracted_SQLData['Changes'] = extracted_SQLData['Changes'].astype(float)
print(extracted_SQLData)





#Function to Calculate Month Average CPI
def CalcChange(Data, Month, Year):
    df = Data[(Data['Date'].dt.month == Month) & (Data['Date'].dt.year == Year)]
    Total_Change = df['Changes'].sum()
    results = {'Date':[str(Year)+"-"+str(Month)], 'Bitcoin Total Change': Total_Change}
    results_df = pd.DataFrame(results)
    return results_df





#Extract Bitcoin Data
Jan20 = CalcChange(extracted_SQLData, 1, 2020)
Feb20 = CalcChange(extracted_SQLData, 2, 2020)
Mar20 = CalcChange(extracted_SQLData, 3, 2020)
Apr20 = CalcChange(extracted_SQLData, 4, 2020)
May20 = CalcChange(extracted_SQLData, 5, 2020)
Jun20 = CalcChange(extracted_SQLData, 6, 2020)
Jul20 = CalcChange(extracted_SQLData, 7, 2020)
Aug20 = CalcChange(extracted_SQLData, 8, 2020)
Sep20 = CalcChange(extracted_SQLData, 9, 2020)
Oct20 = CalcChange(extracted_SQLData, 10, 2020)
Nov20 = CalcChange(extracted_SQLData, 11, 2020)
Dec20 = CalcChange(extracted_SQLData, 12, 2020)
BitcoinData = pd.concat([Jan20, Feb20, Mar20, Apr20, May20, Jun20, Jul20, Aug20, Sep20, Oct20, Nov20, Dec20])





#CPI Extraction
df2 = pd.read_csv('data/US CPI.csv', sep = ',')

#Convert to datetime
df2['Yearmon'] = pd.to_datetime(df2['Yearmon'], format = '%d-%m-%Y')
df2 = df2.sort_values(by='Yearmon', ascending = False)
df2['CPI'] = df2['CPI'].astype(float)





#Create Function to Extract datetime
def CPIChange(Data, Month, Year):
    CurrentMonth = Data[(Data['Yearmon'].dt.month == Month) & (Data['Yearmon'].dt.year == Year)]
    if Month == 1:
        PreviousMonth = Data[(Data['Yearmon'].dt.month == 12) & (Data['Yearmon'].dt.year == Year-1)]
    else:
        PreviousMonth = Data[(Data['Yearmon'].dt.month == Month-1) & (Data['Yearmon'].dt.year == Year)]
    Changes = ((CurrentMonth['CPI'].item() - PreviousMonth['CPI'].item())/(PreviousMonth['CPI'].item()) * 100)
    results = {'Date':[str(Year)+"-"+str(Month)], 'CPI Change': Changes}
    results_df = pd.DataFrame(results)
    return results_df
    




#Extract CPI Data
Jan20 = CPIChange(df2, 1, 2020)
Feb20 = CPIChange(df2, 2, 2020)
Mar20 = CPIChange(df2, 3, 2020)
Apr20 = CPIChange(df2, 4, 2020)
May20 = CPIChange(df2, 5, 2020)
Jun20 = CPIChange(df2, 6, 2020)
Jul20 = CPIChange(df2, 7, 2020)
Aug20 = CPIChange(df2, 8, 2020)
Sep20 = CPIChange(df2, 9, 2020)
Oct20 = CPIChange(df2, 10, 2020)
Nov20 = CPIChange(df2, 11, 2020)
Dec20 = CPIChange(df2, 12, 2020)
CPIData = pd.concat([Jan20, Feb20, Mar20, Apr20, May20, Jun20, Jul20, Aug20, Sep20, Oct20, Nov20, Dec20])





#Merge both CPI and Bitcoin Data
FinalData = pd.merge(BitcoinData, CPIData, how = 'inner')
print(FinalData)





#Plot Barchart
# set width of bar
barWidth = 0.25
fig = plt.subplots(figsize =(18, 12))
# set height of bar
Bitcoin = FinalData['Bitcoin Total Change']
CPI = FinalData['CPI Change']
# Set position of bar on X axis
br1 = np.arange(len(Bitcoin))
br2 = [x + barWidth for x in br1]
# Make the plot
plt.bar(br1, Bitcoin, color ='r', width = barWidth,
        edgecolor ='grey', label ='Bitcoin')
plt.bar(br2, CPI, color ='g', width = barWidth,
        edgecolor ='grey', label ='CPI')
# Adding Xticks
plt.xlabel('Month', fontweight ='bold', fontsize = 15)
plt.ylabel('Percentage Change Month to Month', fontweight ='bold', fontsize = 15)
plt.xticks([r + barWidth for r in range(len(Bitcoin))],
        ['2020-01', '2020-02', '2020-03', '2020-04', '2020-05', '2020-06', '2020-07', '2020-08', '2020-09', '2020-10', '2020-11', '2020-12'])
plt.legend()
plt.show()