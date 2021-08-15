import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine
import pymysql
import os.path

class transform:
    def formatDateId(self, date):
        year = str(date.year)
        month = str(date.month)
        day = str(date.day)
        if (month.__len__() < 2):
            month = '0' + month
        if (day.__len__() < 2):
            day = '0' + day
        return int(year+month+day)

    def formatDate(self, date):
        date = str(date)
        return dt.date(int(date[0:4]), int(date[4: 6]), int(date[6: 8]))

    def __init__(self):
        self.source_file_name = 'customer_order'
        self.databaseName = 'customer_order'
        df_tmp = pd.read_csv(
            f'./data_lake/{self.source_file_name}.csv', parse_dates=['order_date', 'ship_date'])

        self.tableName_customer = 'dim_customer'
        self.costomer_columns = ['customer_id', 'customer_name', 'segment', 'country', 'city',
                                 'state', 'postal_code', 'region']
        self.df_customer = df_tmp[self.costomer_columns]
        self.df_customer = self.df_customer.groupby(
            self.costomer_columns).count().reset_index()
        self.df_customer['status'] = 'active'

        self.tableName_product = 'dim_product'
        self.product_columns = ['product_id',
                                'category', 'sub_category', 'product_name']
        self.df_product = df_tmp[self.product_columns]
        self.df_product = self.df_product.groupby(
            self.product_columns).count().reset_index()
        self.df_product['status'] = 'active'

        self.tableName_date = 'dim_date'
        self.df_date = pd.DataFrame(
            df_tmp['order_date'].append(df_tmp['ship_date']))
        self.df_date.columns = ['date']
        self.df_date['id'] = self.df_date['date'].copy()
        self.df_date['id'] = self.df_date['id'].apply(self.formatDateId).copy()
        self.df_date = self.df_date.groupby(
            ['id', 'date']).count().reset_index()

        self.tableName_order = 'fact_order'
        self.df_order = df_tmp.copy()
        self.df_order['order_date'] = self.df_order['order_date'].apply(
            self.formatDateId).copy()
        self.df_order['ship_date'] = self.df_order['ship_date'].apply(
            self.formatDateId).copy()

    def getNewDateData(self):
        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()
        dim_date = pd.read_sql(
            f'select * from {self.tableName_date}', con=dbConnection)
        dbConnection.close()
        if dim_date.__len__() == 0:
            return self.df_date
        filter = ~((self.df_date['id']).isin(dim_date['id'].tolist()))
        df_tmp = self.df_date.loc[filter].copy()
        return df_tmp

    def getNewCustomerData(self):
        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()

        dim_customer = pd.read_sql(
            f'select * from {self.tableName_customer}', con=dbConnection)
        dbConnection.close()
        if dim_customer.__len__() == 0:
            return self.df_customer
        dim_customer = dim_customer.set_index('id')
        filter = ~((self.df_customer['customer_id'].isin(dim_customer['customer_id'].tolist())) & (
            self.df_customer['customer_name'].isin(dim_customer['customer_name'].tolist())))
        df_tmp = self.df_customer.loc[filter].copy()
        return df_tmp

    def getNewProductData(self):
        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()
        dim_product = pd.read_sql(
            f'select * from {self.tableName_product}', con=dbConnection)
        dbConnection.close()
        if dim_product.__len__() == 0:
            return self.df_product
        dim_product = dim_product.set_index('id')
        filter = ~((self.df_product['product_id'].isin(dim_product['product_id'].tolist())) & (
            self.df_product['product_name'].isin(dim_product['product_name'].tolist())))
        df_tmp = self.df_product.loc[filter].copy()
        return df_tmp

    def getNewOrderData(self):
        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()
        fact_order = pd.read_sql(
            f'select * from {self.tableName_order}', con=dbConnection)
        dbConnection.close()
        if fact_order.__len__() == 0:
            return self.df_order.drop(self.costomer_columns.__add__(self.product_columns), axis=1)
        filter = ~((self.df_order['row_id']).isin(
            fact_order['row_id'].tolist()))
        df_tmp = self.df_order.loc[filter].copy()
        return df_tmp.drop(self.costomer_columns.__add__(self.product_columns), axis=1)

    def getNewOrderDataSql(self):
        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()
        dim_customer = pd.read_sql(
            sql=f'select * from {self.tableName_customer}', con=dbConnection)
        dim_product = pd.read_sql(
            sql=f'select * from {self.tableName_product}', con=dbConnection)
        df_tmp = self.df_order.copy()
        df_tmp = df_tmp.merge(right=dim_customer,
                              how='left', on=self.costomer_columns)
        self.costomer_columns.append('status')
        df_tmp.drop(self.costomer_columns, axis=1, inplace=True)
        df_tmp.rename({'id': 'customer_id'}, axis=1, inplace=True)

        df_tmp = df_tmp.merge(
            right=dim_product, how='left', on=self.product_columns)
        self.product_columns.append('status')
        df_tmp.drop(self.product_columns, axis=1, inplace=True)
        df_tmp.rename({'id': 'product_id'}, axis=1, inplace=True)

        fact_order = pd.read_sql(
            f'select * from {self.tableName_order}', con=dbConnection)
        dbConnection.close()
        if fact_order.__len__() == 0:
            return df_tmp
        filter = ~((df_tmp['row_id']).isin(
            fact_order['row_id'].tolist()))
        return df_tmp[filter]
