import pymysql
import pandas as pd
import datetime
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
from tqdm import tqdm  # 导入 tqdm 库
import re

def execute_sql_and_export_excel(host, port, user, password, database, sql, script_name=None):
    """
    执行 SQL 查询，将结果导出到 Excel，并添加进度打印和时间记录，数据量过大时写入多个 sheet。
    Excel 文件名格式: "database名称" "table名称" "记录当前时间" Y-m-d H.M

    Args:
        host (str): MySQL 服务器主机名。
        port (int): MySQL 服务器端口号。
        user (str): MySQL 用户名。
        password (str): MySQL 密码。
        database (str): 数据库名称。
        sql (str): SQL 查询语句。
        script_name (str, optional): 脚本名称，用于辅助记录或其他用途。默认为 None。
    """
    start_time = datetime.datetime.now()
    print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")

    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        df = pd.read_sql_query(sql, connection)
        total_rows = len(df)
        chunk_size = 1000000
        num_sheets = (total_rows - 1) // chunk_size + 1

        print(f"总数据行数: {total_rows}，将写入 {num_sheets} 个 sheet。")

        # 进度打印
        for i in tqdm(range(0, total_rows, 5), desc="处理数据"):
            print(df.iloc[i:i+5].to_markdown(index=False))

        # 从 SQL 语句中尝试提取表名 (简单提取，可能不适用于所有复杂 SQL)
        table_name_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        table_name = table_name_match.group(1) if table_name_match else "unknown_table"

        # 导出到 Excel
        now = datetime.datetime.now().strftime("%Y-%m-%d %H.%M")
        excel_filename = f"{database} {table_name} {now}.xlsx"
        writer = pd.ExcelWriter(excel_filename, engine='openpyxl')

        for i in range(num_sheets):
            start_row = i * chunk_size
            end_row = min((i + 1) * chunk_size, total_rows)
            df_chunk = df.iloc[start_row:end_row]
            sheet_name = f'Sheet{i + 1}'
            df_chunk.to_excel(writer, sheet_name=sheet_name, index=False)

        writer.close()
        print(f"结果已导出到 {excel_filename}")

        # 打开 Excel 文件并设置格式（只对第一个 sheet 设置冻结和筛选）
        workbook = openpyxl.load_workbook(excel_filename)
        sheet1 = workbook['Sheet1']

        # 冻结首行
        sheet1.freeze_panes = "A2"

        # 设置筛选
        sheet1.auto_filter.ref = f"A1:{get_column_letter(len(df.columns))}{df_chunk.shape[0] + 1}" # 使用 df_chunk 的列数和第一块数据的行数

        workbook.save(excel_filename)
        print(f"{excel_filename} 的 Sheet1 已冻结首行并设置筛选。")

    except pymysql.MySQLError as e:
        print(f"执行 SQL 出错: {e}")
    except Exception as e:
        print(f"导出 Excel 出错: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

    end_time = datetime.datetime.now()
    print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")

# 数据库连接信息
host = '18.178.159.230'
port = 3366
user = 'bigdata'
password = 'uvb5SOSmLH8sCoSU'
database = 'finance_1000'

# SQL 查询语句
sql = """
SELECT
    DATE(created_at) AS created_date,
    member_id, member_username, id,created_at,
    SUM(order_amount) AS total_order_amount,
    SUM(paid_amount) AS total_paid_amount,
    CASE
        WHEN pay_type = 1006 THEN '好博-MPay支付-MPay代收'
        ELSE pay_type
    END AS '支付方式',
    pay_channel, order_status
FROM finance_pay_records
WHERE member_id IN (
    SELECT member_id
    FROM finance_pay_records
    GROUP BY member_id
    HAVING SUM(order_amount) > 0
        AND DATE(created_at) >= '2024-12-30'
)
AND created_at > '2024-01-01 00:00:00'
AND created_at < '2025-12-31 00:00:00'
GROUP BY created_date, member_id, member_username, id, created_at, pay_channel, order_status
ORDER BY created_at DESC, total_order_amount ASC
LIMIT 20 OFFSET 0;
"""

# 执行查询并导出到 Excel
execute_sql_and_export_excel(host, port, user, password, database, sql)