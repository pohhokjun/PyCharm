import os
import zipfile
import io
import pandas as pd
import chardet
import xlsxwriter

def detect_encoding(byte_data):
    """检测文件编码"""
    result = chardet.detect(byte_data)
    return result['encoding'] if result['confidence'] > 0.5 else 'utf-8'

def extract_filename_info(file_name):
    """ 从文件名中提取 5118- 之后 和 行业代表网站域名链接_ 之前的内容 """
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

                            # 读取 CSV，跳过第一行
                            df = pd.read_csv(io.StringIO(raw_data.decode(encoding, errors='ignore')),
                                             encoding=encoding, skiprows=1)

                        if '网站名称' not in df.columns:
                            continue

                        if file_info:
                            df.insert(0, '来源文件', file_info)

                        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def excel_out_oversize(df, file_name):
    """拆分超大数据集导出到 Excel"""
    subsets = [df.iloc[i:i + 1000000] for i in range(0, len(df), 1000000)]
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter', mode='w')

    for i, subset in enumerate(subsets):
        sheet_name = 'Sheet{}'.format(i + 1)
        subset.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.close()

if __name__ == '__main__':
    folder_path = r"C:\Henvita\域名包"  # 你的 ZIP 文件所在路径
    output_path = os.path.join(os.getcwd(), "5118模板站域名筛选.xlsx")  # Excel 输出到当前目录

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

        # 导出 Excel
        excel_out_oversize(data, output_path)

    print("处理完成，数据已导出到:", output_path)
