import pandas as pd
import os
import numpy as np
import datetime as dt
from datetime import datetime
import re
import shutil


# 清空指定文件夹
def clear_folder(folder_path):
    if os.path.exists(folder_path):
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                print(f"删除 {item_path} 时出错: {e}")
    else:
        print(f"文件夹 {folder_path} 不存在")


# 导出Excel文件，冻结首行并启用筛选功能
def excel_out_oversize(df, file_name, date_suffix, output_dir=r'C:\Henvita\1_数据导出'):
    subsets = [df.iloc[i:i + 1000000] for i in range(0, len(df), 1000000)]
    dated_file_name = f"【{file_name.split(' ', 1)[0]}】{file_name.split(' ', 1)[1]} {date_suffix}"
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
        if file.endswith('.txt'):
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

    if filter_leagues:
        leagues = [
            "英格兰超级联赛", "西班牙甲级联赛", "法国甲级联赛", "德国甲级联赛", "意大利甲级联赛",
            "*英格兰超级联赛", "*西班牙甲级联赛", "*法国甲级联赛", "*德国甲级联赛", "*意大利甲级联赛",
            "西班牙甲组联赛", "意大利甲组联赛", "法国甲组联赛", "德国甲组联赛",
            "*西班牙甲组联赛", "*意大利甲组联赛", "*法国甲组联赛", "*德国甲组联赛"
        ]
        pattern = '|'.join(re.escape(l) for l in leagues)
        result = result[
            result['游戏详情'].str.contains(pattern, na=False, regex=True) &
            ~result['游戏详情'].str.contains('独家|虚拟', na=False) &
            result['游戏名称'].str.contains('足球', na=False)
            ]
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
    # 清空文件夹
    clear_folder(r'C:\Henvita\1_数据导出')
    clear_folder(r'C:\Henvita\1_昨日注单数据')

    # 移动文件
    source_folder = r'C:\Users\Administrator\Downloads\Telegram Desktop'
    destination_folder = r'C:\Henvita\1_昨日注单数据'
    os.makedirs(destination_folder, exist_ok=True)
    for filename in os.listdir(source_folder):
        if filename.startswith('昨日') and filename.endswith('_注单数据.txt'):
            try:
                shutil.move(os.path.join(source_folder, filename), os.path.join(destination_folder, filename))
                print(f"已移动文件: {filename} 到 {destination_folder}")
            except Exception as e:
                print(f"移动文件 {filename} 失败: {e}")

    # 获取昨日日期
    yesterday = datetime.now() - dt.timedelta(days=1)
    date_suffix = f'{yesterday.month}.{yesterday.day}'

    # 读取数据
    data = read_txt_to_df(destination_folder)
    if data.empty:
        print("未找到数据文件，程序退出。")
        return
    data['结算日期'] = pd.to_datetime(data['结算日期'], errors='coerce')
    yesterday_date = yesterday.date()

    # 站点和场馆名称映射
    site_map = {
        1000: '好博体育', 2000: '黄金体育', 3000: '宾利体育', 4000: 'HOME体育',
        5000: '亚洲之星', 6000: '玖博体育', 7000: '蓝火体育', 8000: 'A7体育'
    }

    # 数据类型和过滤条件
    data_types = {
        '体育注单数据': (lambda df: process_sports_data(df, filter_leagues=False), None),
        '五大联赛注单': (lambda df: process_sports_data(df, filter_leagues=True), None),
        'GFQP注单数据': (lambda df: df[df['场馆名称'].str.contains('GFQP', na=False)], None),
        'GFDZ注单数据': (
            lambda df: df[df['场馆名称'].str.contains('GFDZ', na=False) & (df['结算日期'].dt.date == yesterday_date)],
            None),
        '电竞注单数据': (process_esports_data, None),
        '棋牌注单数据': (lambda df: df[df['场馆名称'].str.contains('QP', na=False)], None),
        '真人注单数据': (lambda df: df[df['场馆名称'].str.contains('ZR', na=False)], None),
        '捕鱼注单数据': (lambda df: df[df['场馆名称'].str.endswith('BY', na=False)], None),
        '电子注单数据': (lambda df: df[df['场馆名称'].str.contains('DZ|HX', na=False)], None),
        '已结算的AG真人场馆注单数据': (
            lambda df: df[df['场馆名称'].str.contains('AGZR', na=False) & (df['站点ID'] == 2000)], {2000})
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