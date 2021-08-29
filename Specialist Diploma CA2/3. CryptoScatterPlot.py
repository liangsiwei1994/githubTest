#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 14 16:43:57 2021

@author: siweiliang
"""

#PART 3 OF 4
#(RUN FROM PART 1 TO PREVENT ANY ERROR DUE TO DATABASE CREATION)
#REMEMBER TO CLEAR DATABASE AFTER RUNNING ALL SEQUENCE

import plotly.graph_objects as go
import plotly.io as pio
import mysql.connector, sys, pandas as pd





#Set the place to plot be the default browser
pio.renderers.default='browser'





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
  print("Query finished!")
except:
    print("Unexpected error:", sys.exc_info()[0])
finally:
    cursor.close()
    cnx.close()
extracted_SQLData['Changes'] = extracted_SQLData['Changes'].astype(float)

#Convert date in string to datetime object
extracted_SQLData['Date'] = pd.to_datetime(extracted_SQLData['Date'])
extracted_SQLData = extracted_SQLData.sort_values(by='Date')
extracted_SQLData = extracted_SQLData[(extracted_SQLData['Date'].dt.month == 3) & (extracted_SQLData['Date'].dt.year == 2021)]
    
results_dict = {'Average Price':[extracted_SQLData.iloc[:,1].mean()], 'Average Vol':[extracted_SQLData.iloc[:,2].mean()], 'Average Change':[extracted_SQLData.iloc[:,3].mean()], 'Coin':['Bitcoin']}
results = pd.DataFrame(data = results_dict)





#Function to extract data
def ExtractData(filename):
    
    #Get all the crypto data
    df = pd.read_csv("Crypto Data/" + filename, sep = ',', thousands=',', na_values = '-')

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

    #Convert date in string to datetime object
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(by='Date')
    df = df[(df['Date'].dt.month == 3) & (df['Date'].dt.year == 2021)]
    #Add Column and Result
    results_dict = {'Average Price':[df.iloc[:,1].mean()], 'Average Vol':[df.iloc[:,5].mean()], 'Average Change':[df.iloc[:,6].mean()], 'Coin':[filename.split()[0]]}
    results = pd.DataFrame(data = results_dict)

    return results





#Extract all latest coin data
file1 = "Cardano Historical Data.csv"
results1 = ExtractData(file1)
#print(results1)

file2 = "Chainlink Historical Data.csv"
results2 = ExtractData(file2)
#print(results2)

file3 = "Dogecoin Historical Data.csv"
results3 = ExtractData(file3)
#print(results3)

file4 = "Litecoin Historical Data.csv"
results4 = ExtractData(file4)
#print(results4)

file5 = "Matic Historical Data.csv"
results5 = ExtractData(file5)
#print(results5)

file6 = "Polkadot Historical Data.csv"
results6 = ExtractData(file6)
#print(results6)

file7 = "Tether Historical Data.csv"
results7 = ExtractData(file7)
#print(results7)

file8 = "Uniswap Historical Data.csv"
results8 = ExtractData(file8)
#print(results8)

file9 = "XRP Historical Data.csv"
results9 = ExtractData(file9)
#print(results9)





#Concatenate all Data
CombinedResults = pd.concat([results, results1, results2, results3, results4, results5, results6, results7, results8, results9])
CombinedResults = CombinedResults.set_index('Coin')
print(CombinedResults)





#Plot the Graoh
fig = go.Figure(data=go.Scatter(
    x=CombinedResults.index,
    y=CombinedResults['Average Price'],
    mode='markers',
    marker=dict(size=CombinedResults['Average Change'].abs()*[100,100,100,100,100,100,100,1000,100,100], color=[8, 0, 8, 8, 8, 8, 8, 0, 8, 8],),
    ))

fig.update_layout(
    title={
        'text': "Average Price and Average Growth of Major Cryptocurrencies for March 2021",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'})

fig.update_xaxes(title_text='Coin')
fig.update_yaxes(title_text='Price in USD')

fig.show()