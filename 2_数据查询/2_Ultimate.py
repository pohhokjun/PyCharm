import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta

class DatabaseQuery:
    def __init__(self, host: str, port: int, user: str, password: str, site_id: int = 1000,
                 start_date: str = '2025-03-01', end_date: str = '2025-03-31'):
        """初始化数据库连接参数"""
        connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/"
        self.engine = create_engine(connection_string)
        self.site_id = site_id  # 新增 site_id 属性
        self.start_date = start_date
        self.end_date = end_date

    def query_promotion(self) -> pd.DataFrame:
        """查询推广部相关信息"""
        query = """
        SELECT 
            d1.site_id AS '站点ID',
            d1.group_name AS '1级',
            d2.group_name AS '2级',
            d3.group_name AS '3级',
            d4.group_name AS '4级',
            m.agent_name AS '代理名称',
            ma.member_id AS '会员ID'
        FROM agent_1000.agent_department d1
        LEFT JOIN agent_1000.agent_department d2 
            ON d2.pid = d1.id
        LEFT JOIN agent_1000.agent_department d3 
            ON d3.pid = d2.id
        LEFT JOIN agent_1000.agent_department d4 
            ON d4.pid = d3.id
        LEFT JOIN agent_1000.agent_dept_member m
            ON m.dept_id = COALESCE(d4.id, d3.id, d2.id, d1.id)
        LEFT JOIN agent_1000.member_agent ma
            ON ma.agent_name = m.agent_name
        WHERE 
            d1.group_name = '推广部'
            AND d2.group_name IN ('推广1部', '推广3部', '推广5部', '推广6部', '推广7部', '推广9部', '推广11部')
            AND d1.site_id = %s
        """
        return pd.read_sql(query, self.engine, params=(self.site_id,))

    def query_login_members(self) -> pd.DataFrame:
        """查询登入注会员"""
        query = """
        SELECT DISTINCT id AS '会员ID'
        FROM u1_1000.member_info
        WHERE mi.last_login_time > %s
        AND site_id = %s
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.site_id))

    def query_active_low_depositors(self) -> pd.DataFrame:
        """查询在指定日期范围内登录且存款金额大于5000的会员"""
        query = """
        SELECT DISTINCT mds.member_id AS '会员ID'
        FROM bigdata.member_daily_statics mds
        INNER JOIN u1_1000.member_info mi
            ON mds.member_id = mi.id
        WHERE mds.statics_date BETWEEN %s AND %s
            AND mi.last_login_time > %s
            AND mds.site_id = %s
        GROUP BY mds.member_id
        HAVING SUM(mds.draw) < 500
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.end_date, self.start_date, self.site_id))

    def query_recent_login_members(self) -> pd.DataFrame:
        """查询VIP等级>=3且在开始日期前一个月从1号开始登录的会员"""
        start_date_obj = datetime.strptime(self.start_date, '%Y-%m-%d')
        one_month_before = start_date_obj - relativedelta(months=1, day=1)
        day_before_start_date = start_date_obj - relativedelta(days=1)
        query = """
        SELECT DISTINCT id AS '会员ID'
        FROM u1_1000.member_info
        WHERE vip_grade >= 3
        AND last_login_time BETWEEN %s AND %s
        AND site_id = %s
        """
        return pd.read_sql(query, self.engine, params=(one_month_before.strftime('%Y-%m-%d'), day_before_start_date.strftime('%Y-%m-%d'), self.site_id))

    def query_non_betting_members(self) -> pd.DataFrame:
        """查询未投注会员"""
        query = """
        SELECT member_id AS '会员ID'
        FROM bigdata.member_daily_statics
        WHERE statics_date BETWEEN %s AND %s
        AND site_id = %s
        GROUP BY member_id
        HAVING SUM(valid_bet_amount) = 0
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.end_date, self.site_id))

    def query_betting_members(self) -> pd.DataFrame:
        """查询投注金额大于10000的会员"""
        query = """
        SELECT member_id AS '会员ID'
        FROM bigdata.member_daily_statics
        WHERE statics_date BETWEEN %s AND %s
        AND site_id = %s
        GROUP BY member_id
        HAVING SUM(valid_bet_amount) > 10000
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.end_date, self.site_id))

    def query_losing_members(self) -> pd.DataFrame:
        """查询输钱大于3000的会员"""
        query = """
        SELECT member_id AS '会员ID'
        FROM bigdata.member_daily_statics
        WHERE statics_date BETWEEN %s AND %s
        AND site_id = %s
        GROUP BY member_id
        HAVING SUM(profit) > 3000
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.end_date, self.site_id))

    def query_winning_members(self) -> pd.DataFrame:
        """查询赢钱大于3000的会员"""
        query = """
        SELECT member_id AS '会员ID'
        FROM bigdata.member_daily_statics
        WHERE statics_date BETWEEN %s AND %s
        AND site_id = %s
        GROUP BY member_id
        HAVING SUM(profit) < -3000
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.end_date, self.site_id))

    def query_frequent_depositors(self) -> pd.DataFrame:
        """查询存款次数大于3的会员"""
        query = """
        SELECT member_id AS '会员ID'
        FROM bigdata.member_daily_statics
        WHERE statics_date BETWEEN %s AND %s
        AND site_id = %s
        GROUP BY member_id
        HAVING SUM(draw_count) > 3
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.end_date, self.site_id))

    def query_high_depositors(self) -> pd.DataFrame:
        """查询存款金额大于5000的会员"""
        query = """
        SELECT member_id AS '会员ID'
        FROM bigdata.member_daily_statics
        WHERE statics_date BETWEEN %s AND %s
        AND site_id = %s
        GROUP BY member_id
        HAVING SUM(draw) > 5000
        """
        return pd.read_sql(query, self.engine, params=(self.start_date, self.end_date, self.site_id))

def save_to_excel(df: pd.DataFrame, filename: str):
    """保存 DataFrame 到 Excel 文件"""
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        worksheet.freeze_panes(1, 0)
        worksheet.autofilter(0, 0, 0, len(df.columns) - 1)

def work(db_query: DatabaseQuery) -> pd.DataFrame:
    """执行查询并合并结果"""
    result_df = (db_query.query_promotion()
                 .merge(db_query.query_recent_login_members(), on='会员ID', how='left')
                 )
    return result_df

def main():
    start_time = datetime.now()
    print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")

    # 初始化数据库连接
    db_query = DatabaseQuery(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
    )

    # 执行查询
    result = work(db_query)

    # 保存结果
    excel_filename = f"query_result_{start_time.strftime('%Y-%m-%d_%H.%M')}.xlsx"
    save_to_excel(result, excel_filename)
    print(f"结果已保存到: {excel_filename}")

    end_time = datetime.now()
    print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()
