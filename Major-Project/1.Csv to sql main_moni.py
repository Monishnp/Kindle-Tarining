import pandas as pd
import pyodbc
import numpy as np

# Import CSV
data = pd.read_csv("data.csv", encoding='unicode_escape')   ## encoding is used as there are specialchar in the file
df = pd.DataFrame(data)
df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
df = df.fillna(0)




# Connect to SQL Server
conn = pyodbc.connect(Driver='{SQL Server}',
                      Server='34.134.202.81',  # Cloudsqlserver ip
                      Database='MajorProject',
                      UID='sqlserver',
                      PWD='Sandip26@nc',
                      Trusted_Connection='no')
cursor = conn.cursor()


 #Creating Table
cursor.execute(''' 
    CREATE TABLE Sales
(InvoiceNo nvarchar(50),
 StockCode nvarchar(50),
 Description nvarchar(50), 
 Quantity int, 
 InvoiceDate nvarchar(50),
 UnitPrice nvarchar(50), 
 CustomerID float,
 Country nvarchar(50))
    
   ''')  

#insert records into table in groups for better performance

counter=0
#statements = list()
state_list = list()
for row in df.itertuples():
    row_to_insert = [row.InvoiceNo, row.StockCode, row.Description, row.Quantity, row.InvoiceDate, row.UnitPrice,row.CustomerID,row.Country]
    rows_normalized = list(map(lambda x: str(x).replace("'", "''") , row_to_insert))
    statement = "INSERT INTO Sales(InvoiceNo, StockCode, Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')".format(*rows_normalized)
    state_list.append(statement)
    counter += 1
    if counter == 500:
      statements = ';'.join(state_list)
      cursor.execute(statements)
      conn.commit()
      counter =0
      state_list = []

if not counter == 500:
 statements = ';'.join(state_list)
 cursor.execute(statements)
 conn.commit()