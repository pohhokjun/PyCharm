import os
import re
import pandas as pd
import numpy as np
from datetime import datetime


# 读取TXT文件并转换为 DataFrame
def read_txt_to_df(path):
    df_list = []
    for file in os.listdir(path):
        if file.endswith('.txt'):
            file_path = os.path.join(path, file)
            df = pd.read_csv(file_path, sep='\t', encoding='utf-8')
            df_list.append(df)
    return pd.concat(df_list, ignore_index=True)


# 提取 IM体育 的赔率
def extract_odds(row):
    if row['场馆名称'] == 'IMTY':
        match = re.search(r'交易当前的赔率:(\d+\.\d+)#', row['游戏详情'])
        return float(match.group(1)) if match else row['赔率']
    return row['赔率']


# **数据处理**
def process_data(data):
    # 添加游戏类型字段
    data['游戏类型'] = np.nan
    data.loc[data['场馆名称'].str.contains('TY', na=False), '游戏类型'] = '体育'
    data.loc[data['场馆名称'].str.contains('DJ', na=False), '游戏类型'] = '电竞'
    data.loc[data['场馆名称'].str.contains('QP', na=False), '游戏类型'] = '棋牌'
    data.loc[data['场馆名称'].str.contains('ZR', na=False), '游戏类型'] = '真人'
    data.loc[data['场馆名称'].str.endswith('BY', na=False), '游戏类型'] = '捕鱼'
    data.loc[data['场馆名称'].str.contains('DZ|HX', na=False), '游戏类型'] = '电子'

    # 处理体育数据
    sports_data = data[data['游戏类型'] == '体育']
    sports_data['赔率'] = sports_data.apply(extract_odds, axis=1)
    sports_data['赔率类型'] = sports_data['赔率类型'].fillna('')
    sports_data['欧赔'] = np.where(sports_data['赔率类型'] == 'EURO', sports_data['赔率'], sports_data['赔率'] + 1)
    sports_data['联赛名称'] = sports_data['游戏详情'].str.split('\n', expand=True)[1]
    sports_data['球队'] = sports_data['游戏详情'].str.split('\n', expand=True)[2]
    sports_data['玩法'] = sports_data['游戏详情'].str.split('\n', expand=True)[3]
    sports_data = sports_data.drop(columns=['赔率', '赔率类型'])

    return data


# **合并导出 Excel**
def export_merged_data(data, output_file):
    chunk_size = 1_000_000  # Excel 限制最大 1,048,576 行，设置 1,000,000 行一个 Sheet
    num_chunks = (len(data) // chunk_size) + 1

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for i in range(num_chunks):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            sheet_name = f"数据_{i+1}"
            data.iloc[start:end].to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"✅ 导出 {sheet_name}，行数：{len(data.iloc[start:end])}")

    print(f"✅ 全部数据合并导出完成: {output_file}")


# **主执行逻辑**
def main():
    data_folder = r"C:\Henvita\1_昨日注单数据"
    output_file = r"C:\Henvita\Henvita_合拼数据.xlsx"

    print("📥 读取数据...")
    data = read_txt_to_df(data_folder)

    print("🔄 处理数据...")
    processed_data = process_data(data)

    print("📤 合并导出 Excel...")
    export_merged_data(processed_data, output_file)

    print("✅ 全部任务完成！")


if __name__ == "__main__":
    main()
