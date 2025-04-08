import pandas as pd
import datetime
import openpyxl
from openpyxl.utils import get_column_letter
from sqlalchemy import create_engine


# 数据库连接
def get_db_connection(host, port, user, password, database):
    """建立数据库连接"""
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
        return engine
    except Exception as e:
        print(f"连接数据库出错: {e}")
        return None


# 生成 SQL 查询
def generate_main_sql(database, table_name):
    """生成主查询 SQL"""
    return f"""
    SELECT site_id AS 站点ID, statics_date AS 统计日期, 
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
           (SUM(net_amount_settle) + SUM(early_settle_net_amount_settle) + 
            SUM(deposit_adjust_amount) + SUM(dividend_amount) + 
            SUM(per_commission_amount) + SUM(group_profit)) AS 公司净收入
    FROM {database}.{table_name}
    WHERE statics_date >= DATE_SUB(NOW(), INTERVAL 31 DAY) AND statics_date <= NOW()
    GROUP BY site_id, statics_date
    ORDER BY site_id ASC, statics_date ASC
    """


# 查询并返回数据块
def fetch_data_chunks(connection, sql, chunksize=100000):
    """分块查询数据"""
    return pd.read_sql(sql, connection, chunksize=chunksize)


# 写入 Excel 文件
def write_to_excel(chunks, database, table_name):
    """将数据写入 Excel"""
    excel_filename = f"{database} {table_name} {datetime.datetime.now().strftime('%Y-%m-%d %H.%M')}.xlsx"
    writer = pd.ExcelWriter(excel_filename, engine='openpyxl')

    for i, chunk in enumerate(chunks):
        sheet_name = f'Sheet{i + 1}'
        chunk.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.close()
    return excel_filename


# 格式化 Excel 文件
def format_excel_file(excel_filename):
    """格式化 Excel 文件，冻结首行"""
    workbook = openpyxl.load_workbook(excel_filename)
    for sheet in workbook.worksheets:
        sheet.freeze_panes = "A2"
    workbook.save(excel_filename)
    print(f"数据已导出到 {excel_filename}")


# 主流程
def main(host, port, user, password, database, table_name):
    """主执行流程"""
    start_time = datetime.datetime.now()

    connection = get_db_connection(host, port, user, password, database)
    if not connection:
        return

    try:
        sql = generate_main_sql(database, table_name)
        chunks = fetch_data_chunks(connection, sql)
        excel_filename = write_to_excel(chunks, database, table_name)
        format_excel_file(excel_filename)

    finally:
        if connection:
            connection.dispose()

    end_time = datetime.datetime.now()
    print(f"总耗时: {(end_time - start_time).total_seconds():.2f} 秒")


if __name__ == "__main__":
    # 数据库配置
    config = {
        'host': '18.178.159.230',
        'port': 3366,
        'user': 'bigdata',
        'password': 'uvb5SOSmLH8sCoSU',
        'database': 'bigdata',
        'table_name': 'platform_daily_report'
    }

    main(**config)
