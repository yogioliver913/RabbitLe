from unittest import TestCase
from com.init import InitTable
import sqlite3  # 或 pymysql/psycopg2
import pandas as pd
import os
from datetime import datetime

class Test(TestCase):
    def test_batch_insert(self):
        list_row = [
            (1577837800,0.1,0.2,0.3,0.4,0.5),
            (1577847800,0.1,0.2,0.3,0.4,0.5),
            (1579837800,0.1,0.2,0.3,0.4,0.5),
            (2577837800,0.1,0.2,0.3,0.4,0.5),
            (1444837800,0.1,0.2,0.3,0.4,0.5),
            (1577555800,0.2,0.2,0.3,0.4,0.5)
        ]
        InitTable.batch_insert(list_row)

    def test_free_sql(self,sql):
        InitTable.execute(sql)

    def test_convert_dataformat_from_csv(self, date_column='trade_date'):
        #file_path = '/Users/xile/PycharmProjects/RabbitLe/test/SH_MARKET_20160101_20250630.xlsx'
        file_path = '/Users/xile/PycharmProjects/RabbitLe/test/SZ_MARKET_20160101_20250630.xlsx'
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        df = pd.read_excel(file_path)
        df[date_column] = pd.to_datetime(df[date_column], format='%Y%m%d', errors='coerce')
        df['timestamp'] = df[date_column].astype('int64') // 10**9
        if df[date_column].isnull().any():
            print(f"警告：发现 {df[date_column].isnull().sum()} 个无效日期，已自动删除")
            df = df.dropna(subset=[date_column])
        columns = [col for col in df.columns if col not in [date_column, 'timestamp']]  # 排除原日期列
        data_list = list(df[['timestamp'] + columns].itertuples(index=False, name=None))
        InitTable.batch_insert(data_list)




