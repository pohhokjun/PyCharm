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


data = read_txt_to_df(r'C:\Henvita\1_昨日注单数据')
print(data.columns)
print(data)
# 定义日期变量
date_folder = '20'
file_date = '3.19'

# 联赛名称为'游戏详情'的第一行，球队为游戏详情的第二行，以换行符分割

def extract_odds(row):
    if row['场馆名称'] == 'IMTY':
        # 使用正则表达式提取 "交易当前的赔率:" 和 "#" 之间的数字
        match = re.search(r'交易当前的赔率:(\d+\.\d+)#', row['游戏详情'])
        if match:
            return float(match.group(1))  # 提取的赔率转为浮点数
    # 如果不是 IM体育，则返回 df['赔率'] 列中的值
    return row['赔率']

# ----------------------------------------------------------------------------------------------------------------------

sports_data = data[data['场馆名称'].str.contains('TY', na=False)]
sports_data['赔率'] = sports_data.apply(extract_odds, axis=1)

sports_data['赔率类型'] = sports_data['赔率类型'].fillna('')
sports_data['欧赔'] = sports_data['赔率']
# 如果赔率类型为EURO，则欧赔为赔率，否则为赔率+1
sports_data['欧赔'] = np.where(sports_data['赔率类型'] == 'EURO', sports_data['赔率'], sports_data['赔率'] + 1)
sports_data['联赛名称'] = sports_data['游戏详情'].str.split('\n', expand=True)[1]
sports_data['球队'] = sports_data['游戏详情'].str.split('\n', expand=True)[2]
sports_data['玩法'] = sports_data['游戏详情'].str.split('\n', expand=True)[3]

# print(sports_data['联赛名称'])
# print(sports_data['球队'])

print(sports_data.columns)

sports_data = sports_data.drop(columns=['赔率', '赔率类型'])

# ----------------------------------------------------------------------------------------------------------------------
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

# ----------------------------------------------------------------------------------------------------------------------

sports_data_1000 = sports_data[sports_data['站点ID']==1000]
sports_data_2000 = sports_data[sports_data['站点ID']==2000]
sports_data_4000 = sports_data[sports_data['站点ID']==4000]
sports_data_6000 = sports_data[sports_data['站点ID']==6000]
sports_data_7000 = sports_data[sports_data['站点ID']==7000]
sports_data_8000 = sports_data[sports_data['站点ID']==8000]
excel_out_oversize(sports_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的体育注单数据')
excel_out_oversize(sports_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的体育注单数据')
excel_out_oversize(sports_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的体育注单数据')
excel_out_oversize(sports_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的体育注单数据')
excel_out_oversize(sports_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的体育注单数据')
excel_out_oversize(sports_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的体育注单数据')

# ----------------------------------------------------------------------------------------------------------------------

# leagues = [
#     "英格兰超级联赛", "西班牙甲级联赛", "法国甲级联赛", "德国甲级联赛", "意大利甲级联赛",
#     "*英格兰超级联赛", "*西班牙甲级联赛", "*法国甲级联赛", "*德国甲级联赛", "*意大利甲级联赛",
#     "西班牙甲组联赛", "意大利甲组联赛", "法国甲组联赛", "德国甲组联赛",
#     "*西班牙甲组联赛", "*意大利甲组联赛", "*法国甲组联赛", "*德国甲组联赛"
# ]
#
# pattern = '|'.join(re.escape(league) for league in leagues)
# leagues_data = sports_data[
#     sports_data['游戏详情'].str.contains(pattern, na=False, regex=True) &
#     ~sports_data['游戏详情'].str.contains('独家|虚拟', na=False) &
#     sports_data['游戏名称'].str.contains('足球', na=False)
#     ]
#
# leagues_data_1000 = leagues_data[leagues_data['站点ID']==1000]
# leagues_data_2000 = leagues_data[leagues_data['站点ID']==2000]
# leagues_data_4000 = leagues_data[leagues_data['站点ID']==4000]
# leagues_data_6000 = leagues_data[leagues_data['站点ID']==6000]
# leagues_data_7000 = leagues_data[leagues_data['站点ID']==7000]
# leagues_data_8000 = leagues_data[leagues_data['站点ID']==8000]
# excel_out_oversize(leagues_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的五大联赛注单')
# excel_out_oversize(leagues_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的五大联赛注单')
# excel_out_oversize(leagues_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的五大联赛注单')
# excel_out_oversize(leagues_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的五大联赛注单')
# excel_out_oversize(leagues_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的五大联赛注单')
# excel_out_oversize(leagues_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的五大联赛注单')

# --------------------------------------------------------------------------------------------------------------------

# gfgqp_data = data[data['场馆名称'].str.contains('GFDZ', na=False)]
# gfgqp_data_1000 = gfgqp_data[gfgqp_data['站点ID'] == 1000]
# gfgqp_data_2000 = gfgqp_data[gfgqp_data['站点ID'] == 2000]
# gfgqp_data_4000 = gfgqp_data[gfgqp_data['站点ID'] == 4000]
# gfgqp_data_6000 = gfgqp_data[gfgqp_data['站点ID'] == 6000]
# gfgqp_data_7000 = gfgqp_data[gfgqp_data['站点ID'] == 7000]
# gfgqp_data_8000 = gfgqp_data[gfgqp_data['站点ID'] == 8000]
# excel_out_oversize(gfgqp_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的GFDZ注单数据')
# excel_out_oversize(gfgqp_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的GFDZ注单数据')
# excel_out_oversize(gfgqp_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的GFDZ注单数据')
# excel_out_oversize(gfgqp_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的GFDZ注单数据')
# excel_out_oversize(gfgqp_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的GFDZ注单数据')
# excel_out_oversize(gfgqp_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的GFDZ注单数据')

# ----------------------------------------------------------------------------------------------------------------------

esports_data = data[data['场馆名称'].str.contains('DJ', na=False)]
esports_data['赔率类型'] = esports_data['赔率类型'].fillna('')
esports_data['欧赔'] = esports_data['赔率']
# 如果赔率类型为EURO，则欧赔为赔率，否则为赔率+1
esports_data['欧赔'] = np.where(esports_data['赔率类型'] == 'EURO', esports_data['赔率'], esports_data['赔率'] + 1)
# 创建一个布尔掩码，判断场馆名称是否为"LHDJ"
mask = (esports_data['场馆名称'] == 'LHDJ')

# 如果场馆名称 == LHDJ，则使用游戏详情1
esports_data.loc[mask, '联赛名称'] = esports_data.loc[mask, '游戏详情1'].str.split('\n', expand=True)[0]
esports_data.loc[mask, '球队']   = esports_data.loc[mask, '游戏详情1'].str.split('\n', expand=True)[2]
esports_data.loc[mask, '玩法']   = esports_data.loc[mask, '游戏详情1'].str.split('\n', expand=True)[4]

# 如果场馆名称 != LHDJ，则使用游戏详情
esports_data.loc[~mask, '联赛名称'] = esports_data.loc[~mask, '游戏详情'].str.split('\n', expand=True)[1]
esports_data.loc[~mask, '球队']   = esports_data.loc[~mask, '游戏详情'].str.split('\n', expand=True)[2]
esports_data.loc[~mask, '玩法']   = esports_data.loc[~mask, '游戏详情'].str.split('\n', expand=True)[3]

# print(esports_data['联赛名称'])
# print(esports_data['球队'])

print(esports_data.columns)
esports_data = esports_data.drop(columns=['赔率', '赔率类型'])
# 将sports_data输出成excel文件

esports_data_1000 = esports_data[esports_data['站点ID']==1000]
esports_data_2000 = esports_data[esports_data['站点ID']==2000]
esports_data_4000 = esports_data[esports_data['站点ID']==4000]
esports_data_6000 = esports_data[esports_data['站点ID']==6000]
esports_data_7000 = esports_data[esports_data['站点ID']==7000]
esports_data_8000 = esports_data[esports_data['站点ID']==8000]
excel_out_oversize(esports_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的电竞注单数据')
excel_out_oversize(esports_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的电竞注单数据')
excel_out_oversize(esports_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的电竞注单数据')
excel_out_oversize(esports_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的电竞注单数据')
excel_out_oversize(esports_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的电竞注单数据')
excel_out_oversize(esports_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的电竞注单数据')

# --------------------------------------------------------------------------------------------------------------------

# qp_data = data[data['场馆名称'].str.contains('QP', na=False)]
# qp_data_1000 = qp_data[qp_data['站点ID']==1000]
# qp_data_2000 = qp_data[qp_data['站点ID']==2000]
# qp_data_4000 = qp_data[qp_data['站点ID']==4000]
# qp_data_6000 = qp_data[qp_data['站点ID']==6000]
# qp_data_7000 = qp_data[qp_data['站点ID']==7000]
# qp_data_8000 = qp_data[qp_data['站点ID']==8000]
# excel_out_oversize(qp_data_1000, fr'C:\Henvita\2025-02\{date_folder}\好博体育{file_date}已结算的棋牌注单数据')
# excel_out_oversize(qp_data_2000, fr'C:\Henvita\2025-02\{date_folder}\黄金体育{file_date}已结算的棋牌注单数据')
# excel_out_oversize(qp_data_4000, fr'C:\Henvita\2025-02\{date_folder}\HOME体育{file_date}已结算的棋牌注单数据')
# excel_out_oversize(qp_data_6000, fr'C:\Henvita\2025-02\{date_folder}\玖博体育{file_date}已结算的棋牌注单数据')
# excel_out_oversize(qp_data_7000, fr'C:\Henvita\2025-02\{date_folder}\蓝火体育{file_date}已结算的棋牌注单数据')
# excel_out_oversize(qp_data_8000, fr'C:\Henvita\2025-02\{date_folder}\A7体育{file_date}已结算的棋牌注单数据')

# ----------------------------------------------------------------------------------------------------------------------

zr_data = data[data['场馆名称'].str.contains('ZR', na=False)]
zr_data_1000 = zr_data[zr_data['站点ID']==1000]
zr_data_2000 = zr_data[zr_data['站点ID']==2000]
zr_data_4000 = zr_data[zr_data['站点ID']==4000]
zr_data_6000 = zr_data[zr_data['站点ID']==6000]
zr_data_7000 = zr_data[zr_data['站点ID']==7000]
zr_data_8000 = zr_data[zr_data['站点ID']==8000]
excel_out_oversize(zr_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的真人注单数据')
excel_out_oversize(zr_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的真人注单数据')
excel_out_oversize(zr_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的真人注单数据')
excel_out_oversize(zr_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的真人注单数据')
excel_out_oversize(zr_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的真人注单数据')
excel_out_oversize(zr_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的真人注单数据')
# ----------------------------------------------------------------------------------------------------------------------

# hjtyagzr_data = zr_data[(zr_data['场馆名称'].str.contains('AGZR', na=False))&(zr_data['站点ID']==2000)]
# excel_out_oversize(hjtyagzr_data, fr'C:\Henvita\2025-01\{date_folder}\黄金体育{file_date}已结算的AG真人场馆注单数据')

# ----------------------------------------------------------------------------------------------------------------------

by_data = data[data['场馆名称'].str.endswith('BY', na=False)]
by_data_1000 = by_data[by_data['站点ID']==1000]
by_data_2000 = by_data[by_data['站点ID']==2000]
by_data_4000 = by_data[by_data['站点ID']==4000]
by_data_6000 = by_data[by_data['站点ID']==6000]
by_data_7000 = by_data[by_data['站点ID']==7000]
by_data_8000 = by_data[by_data['站点ID']==8000]
excel_out_oversize(by_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的捕鱼注单数据')
excel_out_oversize(by_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的捕鱼注单数据')
excel_out_oversize(by_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的捕鱼注单数据')
excel_out_oversize(by_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的捕鱼注单数据')
excel_out_oversize(by_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的捕鱼注单数据')
excel_out_oversize(by_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的捕鱼注单数据')

# ----------------------------------------------------------------------------------------------------------------------

dz_data = data[data['场馆名称'].str.contains('DZ', na=False)]
dz_data_1000 = dz_data[dz_data['站点ID']==1000]
dz_data_2000 = dz_data[dz_data['站点ID']==2000]
dz_data_4000 = dz_data[dz_data['站点ID']==4000]
dz_data_6000 = dz_data[dz_data['站点ID']==6000]
dz_data_7000 = dz_data[dz_data['站点ID']==7000]
dz_data_8000 = dz_data[dz_data['站点ID']==8000]
excel_out_oversize(dz_data_1000, fr'C:\Henvita\2025-03\{date_folder}\好博体育{file_date}已结算的电子注单数据')
excel_out_oversize(dz_data_2000, fr'C:\Henvita\2025-03\{date_folder}\黄金体育{file_date}已结算的电子注单数据')
excel_out_oversize(dz_data_4000, fr'C:\Henvita\2025-03\{date_folder}\HOME体育{file_date}已结算的电子注单数据')
excel_out_oversize(dz_data_6000, fr'C:\Henvita\2025-03\{date_folder}\玖博体育{file_date}已结算的电子注单数据')
excel_out_oversize(dz_data_7000, fr'C:\Henvita\2025-03\{date_folder}\蓝火体育{file_date}已结算的电子注单数据')
excel_out_oversize(dz_data_8000, fr'C:\Henvita\2025-03\{date_folder}\A7体育{file_date}已结算的电子注单数据')

# ----------------------------------------------------------------------------------------------------------------------

#并且用机器人发送到telegram群

# Telegram bot token and chat_id
# TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
# CHAT_ID = '-1002458775461'
#
# bot = Bot(token=TELEGRAM_BOT_TOKEN)
#
#
# async def send_files_in_folder(bot, folder_path, chat_id):
#     # 遍历文件夹中的所有文件
#     for file_name in os.listdir(folder_path):
#         file_path = os.path.join(folder_path, file_name)
#
#         # 检查是否为文件（忽略子文件夹）
#         if os.path.isfile(file_path):
#             try:
#                 # 发送文件到 chat_id
#                 with open(file_path, 'rb') as file:
#                     await bot.send_document(chat_id=chat_id, document=file)
#                 print(f"文件已发送: {file_name}")
#             except TelegramError as e:
#                 print(f"发送文件 {file_name} 时出错: {e}")
