
import pandas as pd
import os
import numpy as np
import datetime as dt
from datetime import datetime
import re
import shutil


# 导出Excel文件，冻结首行并启用筛选功能
def excel_out_oversize(df, file_name, date_suffix, output_dir=r'C:\Henvita\0_数据导出'):
    subsets = [df.iloc[i:i + 1000000] for i in range(0, len(df), 1000000)]
    # 获取结算日期范围并格式化为 MM.DD-MM.DD
    min_date = pd.to_datetime(df['结算日期']).min().strftime('%m.%d')
    max_date = pd.to_datetime(df['结算日期']).max().strftime('%m.%d')
    date_range = f'{min_date}-{max_date}'
    dated_file_name = f"【{file_name.split(' ', 1)[0]}】{file_name.split(' ', 1)[1]} {date_range}"
    output_path = os.path.join(output_dir, f'{dated_file_name}.xlsx')
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')

    for i, subset in enumerate(subsets):
        subset.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)
        worksheet = writer.sheets[f'Sheet{i + 1}']
        # 冻结首行
        worksheet.freeze_panes(1, 0)
        # 启用筛选功能（范围从第一行第一列到最后一行最后一列）
        worksheet.autofilter(0, 0, subset.shape[0], subset.shape[1] - 1)

    writer.close()


# 读取txt文件到DataFrame
def read_txt_to_df(path):
    df_list = []
    for file in os.listdir(path):
        if file.endswith('_注单数据.txt'):
            with open(os.path.join(path, file), 'r', encoding='utf-8') as f:
                df_list.append(pd.read_csv(f, sep='\t', low_memory=False))
    return pd.concat(df_list) if df_list else pd.DataFrame()


# 提取赔率
def extract_odds(row):
    if row['场馆名称'] == 'IMTY':
        match = re.search(r'交易当前的赔率:(\d+\.\d+)#', row['游戏详情'])
        return float(match.group(1)) if match else row['赔率']
    return row['赔率']


# 处理体育数据
def process_sports_data(df, filter_leagues=False):
    sports_df = df[df['场馆名称'].str.contains('TY', na=False)].copy()
    sports_df.loc[:, '赔率'] = sports_df.apply(extract_odds, axis=1)
    sports_df.loc[:, '赔率类型'] = sports_df['赔率类型'].fillna('')
    sports_df.loc[:, '欧赔'] = np.where(sports_df['赔率类型'] == 'EURO', sports_df['赔率'], sports_df['赔率'] + 1)
    sports_df.loc[:, '联赛名称'] = sports_df['游戏详情'].str.split('\n', expand=True)[1]
    sports_df.loc[:, '球队'] = sports_df['游戏详情'].str.split('\n', expand=True)[2]
    sports_df.loc[:, '玩法'] = sports_df['游戏详情'].str.split('\n', expand=True)[3]
    result = sports_df.drop(columns=['赔率', '赔率类型'])
    return result


# 处理电竞数据
def process_esports_data(df):
    esports_df = df[df['场馆名称'].str.contains('DJ', na=False)].copy()
    esports_df.loc[:, '赔率类型'] = esports_df['赔率类型'].fillna('')
    esports_df.loc[:, '欧赔'] = np.where(esports_df['赔率类型'] == 'EURO', esports_df['赔率'], esports_df['赔率'] + 1)
    mask = esports_df['场馆名称'] == 'LHDJ'
    esports_df.loc[mask, '联赛名称'] = esports_df.loc[mask, '游戏详情1'].str.split('\n', expand=True)[0]
    esports_df.loc[mask, '球队'] = esports_df.loc[mask, '游戏详情1'].str.split('\n', expand=True)[4]
    esports_df.loc[mask, '玩法'] = esports_df.loc[mask, '游戏详情1'].str.split('\n', expand=True)[8]
    esports_df.loc[~mask, '联赛名称'] = esports_df.loc[~mask, '游戏详情'].str.split('\n', expand=True)[1]
    esports_df.loc[~mask, '球队'] = esports_df.loc[~mask, '游戏详情'].str.split('\n', expand=True)[2]
    esports_df.loc[~mask, '玩法'] = esports_df.loc[~mask, '游戏详情'].str.split('\n', expand=True)[3]
    return esports_df.drop(columns=['赔率', '赔率类型'])


# 主处理逻辑
def main():
    # 设置数据源文件夹
    source_folder = r'C:\Users\Administrator\Downloads'

    # 获取昨日日期
    yesterday = datetime.now() - dt.timedelta(days=1)
    date_suffix = f'{yesterday.month}.{yesterday.day}'

    # 读取数据
    data = read_txt_to_df(source_folder)
    if data.empty:
        print("未找到数据文件，程序退出。")
        return
    data['结算日期'] = pd.to_datetime(data['结算日期'], errors='coerce')
    yesterday_date = yesterday.date()

    # 站点和场馆名称映射
    site_map = {
        1000: '好博体育', 2000: '黄金体育', 3000: '宾利体育', 4000: 'HOME体育',
        5000: '亚洲之星', 6000: '玖博体育', 7000: '蓝火体育', 8000: 'A7体育',
        9000: 'K9体育', 9001: '摩根体育', 9002: '友博体育'
    }

    # 数据类型和过滤条件
    data_types = {
        '体育注单数据': (lambda df: process_sports_data(df, filter_leagues=False), None),
        'GFQP注单数据': (lambda df: df[df['场馆名称'].str.contains('GFQP', na=False)], None),
        'GFDZ注单数据': (lambda df: df[df['场馆名称'].str.contains('GFDZ', na=False)],None),
        '电竞注单数据': (process_esports_data, None),
        '棋牌注单数据': (lambda df: df[df['场馆名称'].str.contains('QP', na=False)], None),
        '真人注单数据': (lambda df: df[df['场馆名称'].str.contains('ZR', na=False)], None),
        '捕鱼注单数据': (lambda df: df[df['场馆名称'].str.endswith('BY', na=False)], None),
        '电子注单数据': (lambda df: df[df['场馆名称'].str.contains('DZ|HX', na=False)], None),
        '彩票注单数据': (lambda df: df[df['场馆名称'].str.contains('CP|GGL', na=False)], None),
        '已结算的AG真人场馆注单数据': (lambda df: df[df['场馆名称'].str.contains('AGZR', na=False) & (df['站点ID'] == 2000)], {2000})
    }

    # 处理并导出数据
    for data_type, (processor, site_ids) in data_types.items():
        processed_data = processor(data)
        sites_to_process = site_ids if site_ids else site_map.keys()
        for site_id in sites_to_process:
            site_data = processed_data[processed_data['站点ID'] == site_id]
            if not site_data.empty:
                excel_out_oversize(site_data, f"{site_map[site_id]} {data_type}", date_suffix)


if __name__ == "__main__":
    main()

