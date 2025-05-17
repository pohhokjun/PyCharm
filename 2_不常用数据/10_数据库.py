import pymysql
import pandas as pd
from tqdm import tqdm
from openpyxl.utils import get_column_letter

def get_table_time_range_and_count_to_excel(host, port, user, password, filename='数据库.xlsx'):
    """
    连接到 MySQL 服务器并查询每个表的时间字段的最小值、最大值和总行数，然后将结果导出到 Excel 文件。
    Column Names 分列显示，删除 Column Types，从第2行开始每隔一行插入空行。

    参数：
        host (str): MySQL 服务器主机名或 IP 地址。
        port (int): MySQL 服务器端口号。
        user (str): MySQL 用户名。
        password (str): MySQL 密码。
        filename (str): 要导出的 Excel 文件名。默认为 '数据库.xlsx'。
    """
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        # 获取所有数据库
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()

        all_results = []
        max_columns = 0
        for database in tqdm(databases, desc="Processing Databases"):
            database_name = database[0]
            if database_name in ('information_schema', 'mysql', 'performance_schema', 'sys'):
                continue
            cursor.execute(f"SHOW TABLES FROM {database_name}")
            tables = cursor.fetchall()
            for table in tqdm(tables, desc=f"Processing Tables in {database_name}", leave=False):
                table_name = table[0]
                cursor.execute(f"SHOW COLUMNS FROM {database_name}.{table_name}")
                columns = cursor.fetchall()

                # 查找实际的时间列名
                time_column_name = None
                for column in columns:
                    if 'time' in column[0].lower() or 'date' in column[0].lower():
                        time_column_name = column[0]
                        break

                # 获取表的 Min Time 和 Max Time（如果存在时间字段）
                min_time = None
                max_time = None
                row_count = 0
                if time_column_name:
                    cursor.execute(f"SELECT MIN({time_column_name}), MAX({time_column_name}), COUNT(*) FROM {database_name}.{table_name}")
                    table_result = cursor.fetchone()
                    if table_result:
                        min_time = table_result[0]
                        max_time = table_result[1]
                        row_count = table_result[2]

                # 获取所有列的 Column Names
                column_names = [column[0] for column in columns]
                max_columns = max(max_columns, len(column_names))

                # 构造结果行，补齐列数
                row = [database_name, table_name, row_count, min_time, max_time] + column_names
                # 补齐到最大列数
                row += [None] * (max_columns - len(column_names))
                all_results.append(row)

        # 创建列名
        columns = ['Database', 'Table', 'Row Count', 'Min Time', 'Max Time'] + [f'Column_{i+1}' for i in range(max_columns)]

        # 创建 Pandas DataFrame
        df = pd.DataFrame(all_results, columns=columns)

        # 插入空行：每行数据后加空行
        empty_row = [None] * len(columns)
        new_rows = []
        for row in df.values:
            new_rows.append(row)
            new_rows.append(empty_row)
        df_with_empty = pd.DataFrame(new_rows, columns=columns)

        # 导出到 Excel 文件
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_with_empty.to_excel(writer, index=False, sheet_name='Sheet1', startrow=0)  # 从第1行写入标题
            worksheet = writer.sheets['Sheet1']
            # 冻结首行
            worksheet.freeze_panes = f"{get_column_letter(1)}2"
            # 设置筛选功能
            worksheet.auto_filter.ref = worksheet.dimensions

        print(f"数据已导出到 {filename}")

    except pymysql.MySQLError as e:
        print(f"MySQL 错误: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()

# 使用您的数据库信息调用函数
get_table_time_range_and_count_to_excel(
    host='18.178.159.230',
    port=3366,
    user='bigdata',
    password='uvb5SOSmLH8sCoSU'
)
