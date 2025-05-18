import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import datetime
import openpyxl # 需要这个库来处理Excel格式

# 设置 Google Sheets API 凭证
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
# 请确保你的密钥文件路径正确
creds = ServiceAccountCredentials.from_json_keyfile_name(r"C:\Henvita\hokjun-4e20d0ae306b.json", scope)
client = gspread.authorize(creds)

# 打开 Google Sheet
# 请替换为你的 Google Sheet Key
spreadsheet = client.open_by_key("1TmrAmohUtGBdtZXythGHCLA1U7tZIo8MU6xza_2ZquU")

# 生成动态文件名
script_name = os.path.basename(__file__).replace('.py', '') # 获取脚本文件名（不含扩展名）
now = datetime.datetime.now()
time_str = now.strftime('%m-%d_%H.%M')
excel_filename = f"{script_name}_{time_str}.xlsx"

print(f"准备将数据导出到文件: {excel_filename}")

# 创建 Excel writer 对象
# engine='openpyxl' 允许我们后续访问 openpyxl 对象进行格式设置
with pd.ExcelWriter(excel_filename, engine="openpyxl") as writer:
    # 遍历所有工作表
    for worksheet in spreadsheet.worksheets():
        sheet_name = worksheet.title
        print(f"正在处理工作表: {sheet_name}")
        try:
            # 使用 get_all_records 读取数据，它会自动将第一行识别为标题
            data = worksheet.get_all_records()

            if not data:
                print(f"工作表 {sheet_name} 为空或没有标题行，跳过。")
                continue

            df = pd.DataFrame(data)

            # 保存到 Excel 的一个工作表
            # index=False 表示不写入 DataFrame 的索引
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # 获取当前写入的工作表的 openpyxl 对象
            ws = writer.sheets[sheet_name]

            # 设置冻结首行
            ws.freeze_panes = 'A2' # 冻结 A1 单元格上方的行和左边的列，'A2' 表示冻结第一行

            # 设置筛选功能
            # 获取工作表的有效数据范围，并应用自动筛选
            if ws.max_row > 0 and ws.max_column > 0:
                 ws.auto_filter.ref = ws.dimensions


        except gspread.exceptions.GSpreadException as e:
            print(f"处理工作表 {sheet_name} 时发生 Google Sheets 错误: {e}")
            continue
        except Exception as e:
             print(f"处理工作表 {sheet_name} 时发生未知错误: {e}")
             continue

print(f"数据已成功导出到 {excel_filename}")
