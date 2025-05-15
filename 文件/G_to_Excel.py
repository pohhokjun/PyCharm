
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# 设置 Google Sheets API 凭证
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(r"C:\Henvita\hokjun-5080e816b6a3.json", scope)
client = gspread.authorize(creds)

# 打开 Google Sheet
spreadsheet = client.open_by_key("1TmrAmohUtGBdtZXythGHCLA1U7tZIo8MU6xza_2ZquU")

# 创建 Excel writer 对象
with pd.ExcelWriter("output.xlsx", engine="openpyxl") as writer:
    # 遍历所有工作表
    for worksheet in spreadsheet.worksheets():
        try:
            if worksheet.title == "Sheet6":
                # 对于 Sheet6，使用 get_all_values 绕过标题验证
                data = worksheet.get_all_values()
                if data:
                    headers = data[0]  # 第一行作为标题
                    df = pd.DataFrame(data[1:], columns=headers)  # 其余行作为数据
                else:
                    print(f"Worksheet {worksheet.title} is empty, skipping.")
                    continue
            else:
                # 其他工作表使用 get_all_records
                data = worksheet.get_all_records(expected_headers=None)
                df = pd.DataFrame(data)
            # 保存到 Excel
            df.to_excel(writer, sheet_name=worksheet.title, index=False)
        except gspread.exceptions.GSpreadException as e:
            print(f"Error in worksheet {worksheet.title}: {e}")
            continue

