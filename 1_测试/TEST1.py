import os
import zipfile
import io
import pandas as pd
import chardet
import requests
from bs4 import BeautifulSoup
import whois
import datetime
from tqdm import tqdm
from openpyxl import load_workbook
from multiprocessing import Pool
from pytrends.request import TrendReq

def detect_encoding(byte_data):
    """检测文件编码"""
    result = chardet.detect(byte_data)
    return result['encoding'] if result['confidence'] > 0.5 else 'utf-8'

def extract_filename_info(file_name):
    """从文件名中提取 5118- 之后 和 行业代表网站域名链接_ 之前的内容"""
    start_idx = file_name.find("5118-") + len("5118-") if "5118-" in file_name else -1
    end_idx = file_name.find("行业代表网站域名链接_") if "行业代表网站域名链接_" in file_name else -1
    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        return file_name[start_idx:end_idx]
    return None

def remove_str(string1, string2):
    return string1.replace(string2, '')

def left_trip(string, string2=None):
    return string.lstrip(string2)

def read_zip_csv_skip_first_row(folder_path):
    """读取 ZIP 文件中的 CSV，跳过第一行"""
    dfs = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.zip'):
            file_info = extract_filename_info(file_name)
            with zipfile.ZipFile(os.path.join(folder_path, file_name), 'r') as zip_ref:
                for csv_file_name in zip_ref.namelist():
                    if csv_file_name.endswith('txt') or csv_file_name.endswith('csv'):
                        with zip_ref.open(csv_file_name) as csv_file:
                            raw_data = csv_file.read()
                            encoding = detect_encoding(raw_data)
                            df = pd.read_csv(io.StringIO(raw_data.decode(encoding, errors='ignore')),
                                             encoding=encoding, skiprows=1)
                        if '网站名称' not in df.columns:
                            continue
                        if file_info:
                            df.insert(0, '来源文件', file_info)
                        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def format_url(url):
    """确保 URL 有 http:// 或 https:// 前缀"""
    if not url.startswith("http://") and not url.startswith("https://"):
        return "http://" + url
    return url

def check_responsive(url):
    """检查网站是否为响应式设计"""
    formatted_url = format_url(url)
    try:
        response = requests.get(formatted_url, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        if soup.find('meta', attrs={'name': 'viewport'}):
            return "响应式网站"
        else:
            return "非响应式网站"
    except Exception as e:
        return f"无法访问: {e}"

def get_domain_last_updated(url):
    """获取域名最后更新时间"""
    try:
        domain = url.split("//")[-1].split("/")[0]
        domain_info = whois.whois(domain)
        if isinstance(domain_info.updated_date, list):
            return domain_info.updated_date[0].strftime('%Y-%m-%d %H:%M') if domain_info.updated_date else None
        return domain_info.updated_date.strftime('%Y-%m-%d %H:%M') if domain_info.updated_date else None
    except Exception as e:
        return None

def get_brand_terms(website_name):
    """获取网站名称的品牌词数据"""
    try:
        pytrends = TrendReq(hl='zh-CN', tz=480)
        keywords = [website_name]
        pytrends.build_payload(keywords, cat=0, timeframe='today 5-y', geo='CN', gprop='')
        related_queries = pytrends.related_queries()
        if related_queries and keywords[0] in related_queries:
            top_related = related_queries[keywords[0]]['top']
            if top_related is not None:
                return ', '.join(top_related['query'].tolist())
            else:
                return "没有相关的搜索结果"
        else:
            return "没有相关的搜索结果"
    except Exception as e:
        return f"获取品牌词失败: {e}"

def process_row(row):
    """处理单行数据"""
    row_data = row.to_frame().T
    row_data['响应式检测'] = check_responsive(row['网站名称'])
    row_data['域名更新时间'] = get_domain_last_updated(row['网站名称'])
    row_data['品牌词数据'] = get_brand_terms(row['网站名称']) # 添加品牌词数据列
    return row_data

if __name__ == '__main__':
    folder_path = r"C:\Henvita\域名包"  # 你的 ZIP 文件所在路径
    script_name = os.path.splitext(os.path.basename(__file__))[0]  # 获取脚本名称
    output_path = f"{script_name}_{datetime.datetime.now().strftime('%Y-%m-%d %H%M')}.xlsx"  # Excel 文件名

    start_time = datetime.datetime.now()
    print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")

    # 读取 ZIP 数据并跳过第一行
    data = read_zip_csv_skip_first_row(folder_path)

    # 处理数据
    if not data.empty:
        data['网站名称'] = data['网站名称'].apply(lambda x: remove_str(x, 'www.') if isinstance(x, str) else x)
        data['辅助列'] = data['网站名称'].apply(lambda x: remove_str(x, '.') if isinstance(x, str) else x)
        data['字符长度差'] = data['网站名称'].str.len() - data['辅助列'].str.len()

        # 筛选符合条件的数据
        data1 = data[data['字符长度差'] == 1]
        data2 = data[(data['字符长度差'] == 2) & (data['网站名称'].str.endswith('.com.cn'))]
        data3 = data[(data['字符长度差'] == 2) & (data['网站名称'].str.startswith('m.'))]
        data = pd.concat([data1, data2, data3], ignore_index=True)
        data['网站名称'] = data['网站名称'].apply(lambda x: left_trip(x, 'm.') if isinstance(x, str) else x)

        # 使用 multiprocessing 处理数据
        with Pool() as pool:
            results = list(tqdm(pool.imap(process_row, [row for _, row in data.iterrows()]), total=len(data), desc="处理进度"))
        processed_data = pd.concat(results, ignore_index=True)

        # 保存数据到 Excel
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            processed_data.to_excel(writer, sheet_name='Sheet1', index=False)
        print(f"已保存数据到 Excel 文件: {output_path}")

        # 打印结果
        print(processed_data)

    end_time = datetime.datetime.now()
    print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")
    print("处理完成，数据已导出到:", output_path)