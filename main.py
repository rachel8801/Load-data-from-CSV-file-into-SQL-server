import pandas as pd
import numpy as np
import pyodbc
from APICall import get_export_url


CSV_URL = get_export_url()
Salsifydf = pd.read_csv(CSV_URL, dtype={'UPC':"object",'Features.7':"object",'Features.8':"object",'Masterpiece':"object",
                                        'UPDATED Flat image.1':"object", 'Filter Patterns.3':"object",'Filter Patterns.4':"object",
                                        'Product Colors.6':"object",'Fill Material.1':"object",'Back Material':"object",
                                        'Back Material.1':"object",'Cover Material':"object",
                                        'Cover Material.1':"object"
                                       })

Salsifydf.rename(columns={'USE':'Application','Ready to publish':'ECOMM PUBLISH'}, inplace=True)
df = Salsifydf.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
df = Salsifydf.fillna('')

param_slots = '('+', '.join(['?']*len(df.columns))+')'
columns_ls = df.columns.tolist()
columns_list = f'([{("],[".join(columns_ls))}])'

driver = 'SQL Server'
server = 'sql2019sd.nourison.com'
database = 'Salsify'
username = 'Salsify'
password = 'Tableau$2021'
cnn_string= f'Driver={driver};Server={server};Database={database};UID={username}; PWD={password}'


def update_table():

    with pyodbc.connect(cnn_string) as conn:
        #print('Connected!')
        conn.timeout=3600
        curr = conn.cursor()
        sql = """TRUNCATE TABLE dbo.[all_products]"""
        curr.execute(sql)
        #print("druncated")
        sql2 = "SET ANSI_WARNINGS off"
        curr.execute(sql2)
        ## Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            curr.execute(f""" INSERT INTO dbo.[all_products]{columns_list} values{param_slots}""", row['UPC'], row['Product'],
                         row['ECOMM PUBLISH'], row['Website publish'], row['Workflow Status'],row['Nourison Color'],
                         row['Application'], row['Shape'], row['General Size'], row['Recommended Style'],
                         row['MSRP'], row['Wholesale Price'], row['Designer Price'], row['Stocking Dealer Price'],
                         row['Traffic recommendation'], row['Shedding'], row['Created Date'], row['Construction'],
                         row['Material'], row['BORDER'], row['Features'], row['Features.1'], row['Features.2'],
                         row['Features.3'], row['Features.4'], row['Features.5'], row['Features.6'], row['Features.7'],
                         row['Features.8'], row['Pile description'], row['Pile description.1'],
                         row['Pile description.2'], row['Construction Technique'], row['Masterpiece'],
                         row['WS with cost increase'], row['UPDATED Flat image'], row['UPDATED Flat image.1'],
                         row['UPDATED Flat image.2'], row['UPDATED Flat image.3'], row['UPDATED Flat image.4'],
                         row['UPDATED Flat image.5'], row['UPDATED Flat image.6'],
                         row['Filter Primary Rug Classification'], row['Filter Style'], row['Filter Style.1'],
                         row['Filter Style.2'], row['Filter Style.3'], row['Filter Style.4'], row['Filter Patterns'],
                         row['Filter Patterns.1'], row['Filter Patterns.2'], row['Filter Patterns.3'],
                         row['Filter Patterns.4'], row['Filter Material'], row['Filter Color 1'],
                         row['Filter Color 1.1'], row['Filter Color 1.2'], row['Filter Color 1.3'],
                         row['Filter Color 1.4'], row['Filter Color 1.5'], row['Product Patterns'],
                         row['Product Patterns.1'], row['Product Patterns.2'], row['Product Patterns.3'],
                         row['Product Colors'], row['Product Colors.1'], row['Product Colors.2'],
                         row['Product Colors.3'], row['Product Colors.4'], row['Product Colors.5'],
                         row['Product Colors.6'], row['Product Styles'], row['Product Styles.1'],
                         row['Product Styles.2'], row['Product Styles.3'], row['Product Styles.4'],
                         row['Product Styles.5'], row['Product Styles.6'], row['Product Styles.7'],
                         row['Product Styles.8'], row['Product Styles.9'], row['Non-slip back'], row['Reversible'],
                         row['Latex free'], row['MACHINE WASHABLE?'], row['All-natural'], row['Recycled'],
                         row['JPG Export - Flat image'], row['Fill Material'], row['Fill Material.1'],
                         row['Back Material'], row['Back Material.1'], row['Cover Material'],
                         row['Cover Material.1'])
        conn.commit()
    curr.close()
    conn.close()
    #print('connection closed')

update_table()




