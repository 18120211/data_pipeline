import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine
from transform import transform
import pymysql
import os.path

class load:
    def __init__(self):
        self.databaseName = 'customer_order'
        self.tableName_order = 'fact_order'
        self.tableName_date = 'dim_date'
        self.tableName_customer = 'dim_customer'
        self.tableName_product = 'dim_product'

        self.metadataDBName = 'metadata'
        self.tableName_logger = 'logger'

    def loadCsv(self):
        transform_ins = transform()

        customer_csv = transform_ins.getNewCustomerData()
        product_csv = transform_ins.getNewProductData()
        order_csv = transform_ins.getNewOrderData()

        header_cus_flag = True
        header_pro_flag = True
        header_ord_flag = True

        if os.path.isfile('./data_warehouse/customer.csv'):
            header_cus_flag = False
        if os.path.isfile('./data_warehouse/product.csv'):
            header_pro_flag = False
        if os.path.isfile('./data_warehouse/order.csv'):
            header_ord_flag = False

        if customer_csv.__len__() != 0:
            customer_csv.to_csv('./data_warehouse/customer.csv',
                                mode='a', index=False, header=header_cus_flag)
        if product_csv.__len__() != 0:
            product_csv.to_csv('./data_warehouse/product.csv',
                               mode='a', index=False, header=header_pro_flag)
        if order_csv.__len__() != 0:
            order_csv.to_csv('./data_warehouse/order.csv',
                             mode='a', index=False, header=header_ord_flag)

    def loadDatabase(self):
        transform_ins = transform()

        sqlEngine = create_engine(
            f'mysql+pymysql://root:root@127.0.0.1/{self.databaseName}')
        dbConnection = sqlEngine.connect()

        sqlEngine_metalog = create_engine(f'mysql+pymysql://root:root@127.0.0.1/{self.metadataDBName}')
        dbConnection_metalog = sqlEngine_metalog.connect()

        append_date = transform_ins.getNewDateData()
        if append_date.__len__() != 0:
            append_date.to_sql(name=self.tableName_date,
                               con=dbConnection, if_exists='append', index=False)
            df_tmp = pd.DataFrame({'detail': ['Load dữ liệu ngày tháng vào dim_date'], 'number_row_affected': [append_date.__len__()]})
            df_tmp.to_sql(name=self.tableName_logger, con=dbConnection_metalog, index=False, if_exists='append')
        append_customer = transform_ins.getNewCustomerData()
        if append_customer.__len__() != 0:
            append_customer.to_sql(
                name=self.tableName_customer, con=dbConnection, if_exists='append', index=False)
            df_tmp = pd.DataFrame({'detail': ['Load dữ liệu ngày tháng vào dim_customer'], 'number_row_affected': [append_customer.__len__()]})
            df_tmp.to_sql(name=self.tableName_logger, con=dbConnection_metalog, index=False, if_exists='append')
        append_product = transform_ins.getNewProductData()
        if append_product.__len__() != 0:
            append_product.to_sql(
                name=self.tableName_product, con=dbConnection, if_exists='append', index=False)
            df_tmp = pd.DataFrame({'detail': ['Load dữ liệu ngày tháng vào dim_product'], 'number_row_affected': [append_product.__len__()]})
            df_tmp.to_sql(name=self.tableName_logger, con=dbConnection_metalog, index=False, if_exists='append')
        append_order = transform_ins.getNewOrderDataSql()
        if append_order.__len__() != 0:
            append_order.to_sql(name=self.tableName_order,
                                con=dbConnection, if_exists='append', index=False)
            df_tmp = pd.DataFrame({'detail': ['Load dữ liệu ngày tháng vào fact_order'], 'number_row_affected': [append_order.__len__()]})
            df_tmp.to_sql(name=self.tableName_logger, con=dbConnection_metalog, index=False, if_exists='append')
        dbConnection.close()
        dbConnection_metalog.close()