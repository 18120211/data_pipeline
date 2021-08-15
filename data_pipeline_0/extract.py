import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine
import pymysql
import os.path

class extract:
    def __init__(self, source_file_name=str):
        self.tableName = 'row_data'
        self.databaseName = 'customer_order_raw'
        self.source_file_name = source_file_name
        self.dataFrame = pd.read_csv(f'./source_system/{self.source_file_name}.csv',
                     parse_dates=['Order Date', 'Ship Date'])
        self.columns = ['row_id', 'order_id', 'order_date', 'ship_date', 'ship_mode', 'customer_id', 'customer_name', 'segment', 'country', 'city',
            'state', 'postal_code', 'region', 'product_id', 'category', 'sub_category', 'product_name', 'sales', 'quantity', 'discount',  'profit']
        self.dataFrame.columns = self.columns
        self.dataFrame = self.dataFrame.groupby(self.columns).count().reset_index()

    def extractData(self):
        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()

        cur_dataFrame = pd.read_sql(f'select * from {self.tableName}', con=dbConnection)
        filter = ~(self.dataFrame['row_id'].isin(cur_dataFrame['row_id']))
        self.dataFrame = self.dataFrame.loc[filter]
        header_flag = True
        if os.path.isfile('./data_lake/customer_order.csv'):
            header_flag = False
        self.dataFrame.to_csv('./data_lake/customer_order.csv', mode = 'a', index=False, header=header_flag)
            

        try:
            self.dataFrame.to_sql(self.tableName, con = sqlEngine, if_exists='append', index=False)
        except ValueError as ex:
            print(ex)
        finally:
            dbConnection.close()

    def getLakeDataFrame(self):
        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()
        frame = pd.read_sql(f'select * from {self.tableName}', con=dbConnection)
        return frame

