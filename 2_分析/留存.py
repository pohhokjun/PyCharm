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
    """获取数据，按 site_id 分组"""
    sql = f"""
    WITH date_range AS (
        SELECT DATE_SUB(DATE(NOW()), INTERVAL n DAY) AS statics_date
        FROM (SELECT ROW_NUMBER() OVER () - 1 AS n FROM information_schema.columns LIMIT 31) t
    ),
    base_data AS (
        SELECT site_id, 
               DATE(first_deposit_time) AS cohort_date, 
               member_id, 
               bets, 
               DATE(statics_date) AS activity_date
        FROM {database}.{table_name}
        WHERE first_deposit_time BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND NOW()
          AND statics_date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND NOW()
    ),
    new_users AS (
        SELECT site_id, cohort_date, COUNT(DISTINCT member_id) AS new_users
        FROM base_data
        WHERE bets > 0
        GROUP BY site_id, cohort_date
    ),
    retention AS (
        SELECT site_id, 
               activity_date AS statics_date,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 2 DAY) 
                    AND bets > 0 THEN member_id END) AS retention_3d,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 6 DAY) 
                    AND bets > 0 THEN member_id END) AS retention_7d,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 14 DAY) 
                    AND bets > 0 THEN member_id END) AS retention_15d,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 29 DAY) 
                    AND bets > 0 THEN member_id END) AS retention_30d
        FROM base_data
        GROUP BY site_id, activity_date
    )
    SELECT d.statics_date, 
           COALESCE(n.site_id, r.site_id) AS 站点ID, 
           COALESCE(n.new_users, 0) AS 新增用户数,
           COALESCE(r.retention_3d, 0) AS 第3天留存用户数,
           COALESCE(r.retention_7d, 0) AS 第7天留存用户数,
           COALESCE(r.retention_15d, 0) AS 第15天留存用户数,
           COALESCE(r.retention_30d, 0) AS 第30天留存用户数
    FROM date_range d
    LEFT JOIN new_users n ON d.statics_date = n.cohort_date
    LEFT JOIN retention r ON d.statics_date = r.statics_date AND (n.site_id = r.site_id OR (n.site_id IS NULL AND r.site_id IS NULL))
    ORDER BY 站点ID, d.statics_date ASC
    """
    try:
        return pd.read_sql_query(sql, connection, chunksize=50000)  # 减小 chunksize 以降低内存占用
    except Exception as e:
        print(f"查询出错: {e}")
        return None


# Excel 操作
def write_to_excel(chunks, database, table_name):
    """写入 Excel"""
    now = datetime.datetime.now().strftime("%Y-%m-%d %H.%M")
    filename = f"{database}_{table_name}_{now}.xlsx"
    writer = pd.ExcelWriter(filename, engine='openpyxl')
    total_rows = 0

    for i, chunk in enumerate(tqdm(chunks, desc="写入 Excel")):
        total_rows += len(chunk)
        chunk.to_excel(writer, sheet_name=f'Sheet{i + 1}', index=False)
        writer.sheets[f'Sheet{i + 1}'].freeze_panes = 'A2'  # 在写入时直接设置冻结
        writer.sheets[f'Sheet{i + 1}'].auto_filter.ref = (
            f"A1:{get_column_letter(chunk.shape[1])}{chunk.shape[0] + 1}"
        )

    writer.close()
    print(f"总行数: {total_rows}，导出至 {filename}")
    return filename, total_rows


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

    end_time = datetime.datetime.now()
    print(f"结束: {end_time.strftime('%Y-%m-%d %H:%M')}, 耗时: {(end_time - start_time).total_seconds():.2f} 秒")


if __name__ == "__main__":
    main()
