import pymysql
import pandas as pd
import datetime
import openpyxl
from openpyxl.utils import get_column_letter
from tqdm import tqdm
import re


# SQL 操作部分
def get_db_connection(host, port, user, password, database):
    """建立数据库连接"""
    try:
        return pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    except pymysql.MySQLError as e:
        print(f"连接数据库出错: {e}")
        return None


def fetch_member_ids(connection, database, table_name):
    """查询满足条件的 member_ids"""
    sql = f"""
    SELECT member_id
    FROM {database}.{table_name}
    WHERE DATE(created_at) >= '2024-12-30'
    GROUP BY member_id
    HAVING SUM(order_amount) > 0
    """
    try:
        df = pd.read_sql_query(sql, connection)
        return tuple(df['member_id'].tolist())
    except Exception as e:
        print(f"获取 member_ids 出错: {e}")
        return ()


def fetch_main_data(connection, member_ids, database, table_name):
    """执行主查询并返回数据"""
    if not member_ids:  # 如果 member_ids 为空，移除 IN 条件
        sql = f"""
        SELECT
                site_id AS 站点ID,
                statics_date AS 统计日期,
                SUM(register_member_count) AS 注册人数,
                SUM(first_recharge_member_count) AS 首充人数,
                SUM(first_recharge_amount) AS 首充金额,
                SUM(recharge_member_count) AS 充值人数,
                SUM(recharge_amount) AS 存款金额,
                SUM(drawing_member_count) AS 取款人数,
                SUM(drawing_amount) AS 取款金额,
                SUM(recharge_drawing_sub) AS 存提差,
                SUM(bet_member_count_settle) AS 投注人数,
                SUM(valid_bet_amount_settle) AS 投注金额,
                SUM(net_amount_settle) AS 公司输赢,
                SUM(early_settle_net_amount_settle) AS 提前结算,
                SUM(deposit_adjust_amount) AS 账户调整,
                SUM(dividend_amount) AS 红利,
                SUM(rebate_amount) AS 返水,
                SUM(per_commission_amount) AS 代理佣金,
                AVG(group_amount_ratio) AS 集团分成比例,
                SUM(group_profit) AS 集团分成,
                (SUM(net_amount_settle) + SUM(early_settle_net_amount_settle) + SUM(deposit_adjust_amount) + SUM(dividend_amount) + SUM(per_commission_amount) + SUM(group_profit)) AS 公司净收入
        FROM {database}.{table_name}
        AND statics_date >= DATE_SUB(NOW(), INTERVAL 31 DAY)
        AND statics_date <= NOW()
        GROUP BY site_id, statics_date
        ORDER BY site_id ASC, statics_date ASC;
        """
    else:
        sql = f"""
        SELECT
            DATE(created_at) AS created_date,
            site_id, member_id, member_username, id, created_at,
            SUM(order_amount) AS total_order_amount,
            CASE
                WHEN pay_type = 1006 THEN '好博-MPay支付-MPay代收'
                ELSE pay_type
            END AS '支付方式',
            pay_channel, order_status
        FROM {database}.{table_name}
        WHERE member_id IN {member_ids}
        AND created_at > '2024-12-30 00:00:00'
        AND created_at < '2025-01-01 23:59:59'
        AND site_id = 2000
        AND order_status = -1
        GROUP BY created_date, member_id, member_username, id, created_at, pay_channel, order_status, site_id
        ORDER BY created_at ASC, total_order_amount DESC
        LIMIT 9999999999 OFFSET 0
        """
    try:
        return pd.read_sql_query(sql, connection, chunksize=1000000)
    except Exception as e:
        print(f"执行主查询出错: {e}")
        return None


# Excel 操作部分
def extract_table_name(sql):
    """从 SQL 查询中提取表名"""
    match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
    return match.group(1) if match else "unknown_table"


def generate_excel_filename(database, table_name):
    """生成 Excel 文件名"""
    now = datetime.datetime.now().strftime("%Y-%m-%d %H.%M")
    return f"{database} {table_name} {now}.xlsx"


def write_to_excel(chunks, excel_filename):
    """将数据写入 Excel"""
    start_time = datetime.datetime.now()
    print(f"Excel 写入开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")

    writer = pd.ExcelWriter(excel_filename, engine='openpyxl')
    total_rows = 0

    for i, df_chunk in enumerate(tqdm(chunks, desc="Writing to Excel")):
        total_rows += len(df_chunk)
        sheet_name = f'Sheet{i + 1}'
        df_chunk.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.close()
    print(f"总数据行数: {total_rows}，已导出到 {excel_filename}")
    return total_rows


def format_excel(excel_filename, chunk_size, total_rows, column_count):
    """格式化 Excel 文件"""
    workbook = openpyxl.load_workbook(excel_filename)
    for sheet_name in workbook.sheetnames:
        sheet = workbook[sheet_name]
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = f"A1:{get_column_letter(column_count)}{min(chunk_size, total_rows) + 1}"
    workbook.save(excel_filename)
    print(f"{excel_filename} 的所有 sheet 已冻结首行并设置筛选。")


# 主流程
def main():
    # 数据库连接信息
    host = '18.178.159.230'
    port = 3366
    user = 'bigdata'
    password = 'uvb5SOSmLH8sCoSU'

    # 定义数据库和表名，便于统一修改
    database = 'bigdata'
    table_name = 'platform_daily_report'

    # 添加开关变量，控制是否使用 fetch_member_ids
    use_fetch_member_ids = False  # 设置为 True 使用，False 则跳过

    # SQL 查询部分
    start_time = datetime.datetime.now()
    print(f"SQL 查询开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")

    connection = get_db_connection(host, port, user, password, database)
    if not connection:
        return

    try:
        if use_fetch_member_ids:
            member_ids = fetch_member_ids(connection, database, table_name)
            if not member_ids:
                print("未获取到有效的 member_ids")
                # 继续执行，但 member_ids 为空时 fetch_main_data 会处理
        else:
            # 如果不使用 fetch_member_ids，设置为空元组
            member_ids = tuple()

        chunks = fetch_main_data(connection, member_ids, database, table_name)
        if chunks is None:
            return

        # Excel 处理部分
        table_name_from_sql = extract_table_name(f"FROM {table_name}")
        excel_filename = generate_excel_filename(database, table_name_from_sql)
        total_rows = write_to_excel(chunks, excel_filename)

        # 额外查询以获取列数，检查 member_ids 是否为空
        if not member_ids:
            sample_sql = f"SELECT * FROM {database}.{table_name} LIMIT 1"
        else:
            sample_sql = f"SELECT * FROM {database}.{table_name} WHERE member_id IN {member_ids} LIMIT 1"
        sample_chunk = pd.read_sql_query(sample_sql, connection)
        column_count = len(sample_chunk.columns)
        format_excel(excel_filename, 1000000, total_rows, column_count)

    finally:
        if connection:
            connection.close()

    end_time = datetime.datetime.now()
    duration = end_time - start_time
    print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"总耗时: {duration.total_seconds():.2f} 秒")


if __name__ == "__main__":
    main()
