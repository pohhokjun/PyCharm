import csv
import zipfile
import io
import pandas as pd
import os
import numpy as np
import datetime as dt
import openpyxl
import zipfile2
from pandasql import sqldf
from datetime import datetime
import asyncio
# from telegram import Bot
# from telegram.error import TelegramError
import re

#读取文件夹中的所有txt文件到df

def read_txt_to_df(path):
    df_list = []
    for file in os.listdir(path):
        if file.endswith('.txt'):
            with open(os.path.join(path, file), 'r', encoding='utf-8') as f:
                df = pd.read_csv(f, sep='\t')
                df_list.append(df)
    return pd.concat(df_list)


data = read_txt_to_df(r'C:\Henvita\gfdz')
print(data.columns)
print(data)
# 定义日期变量
date_folder = '20'
file_date = '3.19'

# 将sports_data输出成excel文件
def excel_out_oversize(df, file_name):
    subsets = [df.iloc[i:i + 1000000] for i in range(0, len(df), 1000000)]

    # 创建一个新的Excel文件
    writer = pd.ExcelWriter(file_name + '.xlsx', engine='xlsxwriter')

    # 将每个子集写入不同的Excel工作表中
    for i, subset in enumerate(subsets):
        sheet_name = 'Sheet{}'.format(i + 1)
        subset.to_excel(writer, sheet_name=sheet_name, index=False)

    # 关闭Excel写入器
    writer.close()

gfgqp_data = data[data['场馆名称'].str.contains('GFDZ', na=False)]
gfgqp_data_1000 = gfgqp_data[gfgqp_data['站点ID'] == 1000]
gfgqp_data_2000 = gfgqp_data[gfgqp_data['站点ID'] == 2000]
gfgqp_data_4000 = gfgqp_data[gfgqp_data['站点ID'] == 4000]
gfgqp_data_6000 = gfgqp_data[gfgqp_data['站点ID'] == 6000]
gfgqp_data_7000 = gfgqp_data[gfgqp_data['站点ID'] == 7000]
gfgqp_data_8000 = gfgqp_data[gfgqp_data['站点ID'] == 8000]
excel_out_oversize(gfgqp_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的GFDZ注单数据')
excel_out_oversize(gfgqp_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的GFDZ注单数据')
excel_out_oversize(gfgqp_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的GFDZ注单数据')
excel_out_oversize(gfgqp_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的GFDZ注单数据')
excel_out_oversize(gfgqp_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的GFDZ注单数据')
excel_out_oversize(gfgqp_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的GFDZ注单数据')