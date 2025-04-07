import pymysql
import pandas as pd
import datetime
from openpyxl.utils import get_column_letter
from tqdm import tqdm


# 数据库操作
def get_db_connection(config):
    """建立数据库连接"""
    try:
        return pymysql.connect(**config)
    except pymysql.MySQLError as e:
        print(f"连接数据库出错: {e}")
        return None


def fetch_data(connection, database, table_name):
    """获取数据"""
    sql = f"""
    WITH date_range AS (
        SELECT DATE_SUB(DATE(NOW()), INTERVAL n DAY) AS statics_date
        FROM (SELECT ROW_NUMBER() OVER () - 1 AS n FROM information_schema.columns LIMIT 31) t
    ),
    new_users AS (
        SELECT site_id, DATE(first_deposit_time) AS cohort_date, COUNT(DISTINCT member_id) AS new_users
        FROM {database}.{table_name}
        WHERE first_deposit_time BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND NOW()
        GROUP BY site_id, DATE(first_deposit_time)
    ),
    activity AS (
        SELECT site_id, DATE(statics_date) AS statics_date, member_id, bets
        FROM {database}.{table_name}
        WHERE statics_date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND NOW()
    ),
    retention AS (
        SELECT d.statics_date AS cohort_date,
               COUNT(DISTINCT CASE WHEN DATE(n.first_deposit_time) = DATE_SUB(d.statics_date, INTERVAL 2 DAY) 
                    AND a.bets > 0 THEN a.member_id END) AS retention_3d,
               COUNT(DISTINCT CASE WHEN DATE(n.first_deposit_time) = DATE_SUB(d.statics_date, INTERVAL 6 DAY) 
                    AND a.bets > 0 THEN a.member_id END) AS retention_7d,
               COUNT(DISTINCT CASE WHEN DATE(n.first_deposit_time) = DATE_SUB(d.statics_date, INTERVAL 14 DAY) 
                    AND a.bets > 0 THEN a.member_id END) AS retention_15d,
               COUNT(DISTINCT CASE WHEN DATE(n.first_deposit_time) = DATE_SUB(d.statics_date, INTERVAL 29 DAY) 
                    AND a.bets > 0 THEN a.member_id END) AS retention_30d
        FROM date_range d
        LEFT JOIN {database}.{table_name} n ON DATE(n.first_deposit_time) BETWEEN DATE_SUB(d.statics_date, INTERVAL 29 DAY) AND d.statics_date
        LEFT JOIN activity a ON n.member_id = a.member_id AND DATE(a.statics_date) = d.statics_date
        GROUP BY d.statics_date
    )
    SELECT n.site_id AS 站点ID, d.statics_date, COALESCE(n.new_users, 0) AS 新增用户数,
           COALESCE(r.retention_3d, 0) AS 第3天留存用户数, COALESCE(r.retention_7d, 0) AS 第7天留存用户数,
           COALESCE(r.retention_15d, 0) AS 第15天留存用户数, COALESCE(r.retention_30d, 0) AS 第30天留存用户数
    FROM date_range d
    LEFT JOIN new_users n ON d.statics_date = n.cohort_date
    LEFT JOIN retention r ON d.statics_date = r.cohort_date
    ORDER BY n.site_id, d.statics_date ASC
    """
    try:
        return pd.read_sql_query(sql, connection, chunksize=1000000)
    except Exception as e:
        print(f"查询出错: {e}")
        return None


# Excel 操作
def write_to_excel(chunks, database, table_name):
    """写入 Excel"""
    now = datetime.datetime.now().strftime("%Y-%m-%d %H.%M")
    filename = f"{database} {table_name} {now}.xlsx"
    writer = pd.ExcelWriter(filename, engine='openpyxl')
    total_rows = 0

    for i, chunk in enumerate(tqdm(chunks, desc="写入 Excel")):
        total_rows += len(chunk)
        chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)

    writer.close()
    print(f"总行数: {total_rows}，导出至 {filename}")
    return filename, total_rows


def format_excel(filename):
    """格式化 Excel"""
    from openpyxl import load_workbook
    wb = load_workbook(filename)
    for sheet in wb:
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = f"A1:{get_column_letter(sheet.max_column)}{sheet.max_row}"
    wb.save(filename)
    print(f"{filename} 已格式化")


# 主流程
def main():
    config = {
        'host': '18.178.159.230',
        'port': 3366,
        'user': 'bigdata',
        'password': 'uvb5SOSmLH8sCoSU',
        'database': 'bigdata'
    }
    table_name = 'member_daily_statics'

    start_time = datetime.datetime.now()
    print(f"开始: {start_time.strftime('%Y-%m-%d %H:%M')}")

    with get_db_connection(config) as conn:
        if not conn:
            return

        chunks = fetch_data(conn, config['database'], table_name)
        if chunks:
            filename, total_rows = write_to_excel(chunks, config['database'], table_name)
            format_excel(filename)

    end_time = datetime.datetime.now()
    print(f"结束: {end_time.strftime('%Y-%m-%d %H:%M')}, 耗时: {(end_time - start_time).total_seconds():.2f} 秒")


if __name__ == "__main__":
    main()
