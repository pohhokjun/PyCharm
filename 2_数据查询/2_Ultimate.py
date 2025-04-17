import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dateutil.relativedelta import relativedelta


class DatabaseQuery:
    def __init__(self, host: str, port: int, user: str, password: str, site_id: int = 1000,
                 start_date: str = '2025-03-01', end_date: str = '2025-03-31',
                 agent_1000: str = 'agent_1000', u1_1000: str = 'u1_1000',
                 bigdata: str = 'bigdata', control_1000: str = 'control_1000'):
        """初始化数据库连接参数"""
        connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/"
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.control_1000 = control_1000
        self.bigdata = bigdata
        self.agent_1000 = agent_1000
        self.u1_1000 = u1_1000
        self.site_id = site_id  # 站点ID
        self.start_date = start_date  # 开始日期
        self.end_date = end_date  # 结束日期

    def close_session(self):
        """关闭当前会话"""
        if self.session:
            self.session.close()

    def Custom(self) -> pd.DataFrame:
        """查询登入注会员"""
        query = """

        """
        return pd.read_sql(query, self.engine)

    def _1_query_member_stats(self) -> pd.DataFrame:
        """查询会员历史累计统计信息"""
        query = f"""
        SELECT
            u1_mi.id AS '会员ID',
            u1_mi.name AS '会员账号',
            u1_mi.vip_grade AS 'VIP等级',
            GROUP_CONCAT(c1_sv.dict_value ORDER BY c1_sv.code SEPARATOR ',') AS '标签',
            u1_mi.created_at AS '注册时间',
            u1_mi.last_login_time AS '最后登录时间',
            COALESCE(SUM(b_mds.deposit), 0) AS '历史累计存款',
            COALESCE(SUM(b_mds.draw), 0) AS '历史累计取款',
            COALESCE(SUM(b_mds.bets), 0) AS '历史有效投注金额',
            COALESCE(-SUM(b_mds.profit), 0) AS '历史总输赢'
        FROM {self.u1_1000}.member_info u1_mi
        LEFT JOIN {self.control_1000}.sys_dict_value c1_sv
            ON FIND_IN_SET(c1_sv.code, u1_mi.tag_id)
        LEFT JOIN {self.bigdata}.member_daily_statics b_mds
            ON u1_mi.id = b_mds.member_id
        WHERE u1_mi.status <> 0
            AND (c1_sv.initial_flag IS NULL OR c1_sv.initial_flag <> 1)
        GROUP BY u1_mi.id,u1_mi.name,u1_mi.vip_grade,u1_mi.created_at,u1_mi.last_login_time
        """
        return pd.read_sql(query, self.engine)

    def _10_query_promotion(self) -> pd.DataFrame:
        """查询推广部相关信息"""
        query = f"""
        SELECT
            a1_ad.site_id AS '站点ID',
            a1_ad.group_name AS '1级',
            a1_ad_2.group_name AS '2级',
            a1_ad_3.group_name AS '3级',
            a1_ad_4.group_name AS '4级',
            a1_adm.agent_name AS '代理名称',
            u1_mi.id AS '会员ID'
        FROM {self.agent_1000}.agent_department a1_ad
        LEFT JOIN {self.agent_1000}.agent_department a1_ad_2
            ON a1_ad_2.pid = a1_ad.id
        LEFT JOIN {self.agent_1000}.agent_department a1_ad_3
            ON a1_ad_3.pid = a1_ad_2.id
        LEFT JOIN {self.agent_1000}.agent_department a1_ad_4
            ON a1_ad_4.pid = a1_ad_3.id
        LEFT JOIN {self.agent_1000}.agent_dept_member a1_adm
            ON a1_adm.dept_id = COALESCE(a1_ad_4.id, a1_ad_3.id, a1_ad_2.id, a1_ad.id)
        LEFT JOIN u1_1000.member_info u1_mi
            ON u1_mi.top_name = a1_adm.agent_name
        WHERE
            a1_ad.group_name = '推广部'
            AND a1_ad_2.group_name IN ('推广1部', '推广3部', '推广5部', '推广6部', '推广7部', '推广9部', '推广11部')
            AND a1_ad.site_id = {self.site_id}
        """
        return pd.read_sql(query, self.engine)

    def _11_query_login_members(self) -> pd.DataFrame:
        """查询登入注会员"""
        query = f"""
        SELECT DISTINCT u1_mi.id AS '会员ID'
        FROM {self.u1_1000}.member_info u1_mi
        WHERE u1_mi.last_login_time > '{self.start_date}'
        AND u1_mi.site_id = {self.site_id}
        """
        return pd.read_sql(query, self.engine)

    def _12_query_active_low_depositors(self) -> pd.DataFrame:
        """查询在指定日期范围内登录且存款金额大于5000的会员"""
        query = f"""
        SELECT DISTINCT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        INNER JOIN {self.u1_1000}.member_info u1_mi
            ON b_mds.member_id = u1_mi.id
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
            AND u1_mi.last_login_time > '{self.start_date}'
            AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.draw) < 500
        """
        return pd.read_sql(query, self.engine)

    def _14_query_recent_login_members(self) -> pd.DataFrame:
        """查询VIP等级>=3且在开始日期前一个月从1号开始登录的会员"""
        start_date_obj = datetime.strptime(self.start_date, '%Y-%m-%d')
        one_month_before = start_date_obj - relativedelta(months=1, day=1)
        day_before_start_date = start_date_obj - relativedelta(days=1)
        query = f"""
        SELECT DISTINCT u1_mi.id AS '会员ID'
        FROM {self.u1_1000}.member_info u1_mi
        WHERE u1_mi.vip_grade >= 3
        AND u1_mi.last_login_time BETWEEN '{one_month_before.strftime('%Y-%m-%d')}' AND '{day_before_start_date.strftime('%Y-%m-%d')}'
        AND u1_mi.site_id = {self.site_id}
        """
        return pd.read_sql(query, self.engine)

    def _15_merge_login_non_betting_members(self) -> pd.DataFrame:
        """使用 SQL INNER JOIN 合并登录会员和未投注会员查询，获取同时满足条件的会员ID"""
        query = f"""
        SELECT DISTINCT u1_mi.id AS '会员ID'
        FROM {self.u1_1000}.member_info u1_mi
        INNER JOIN {self.bigdata}.member_daily_statics b_mds
        ON u1_mi.id = b_mds.member_id
        WHERE u1_mi.last_login_time > '{self.start_date}'
        AND u1_mi.site_id = {self.site_id}
        AND b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY u1_mi.id
        HAVING SUM(b_mds.valid_bet_amount) = 0
        """
        return pd.read_sql(query, self.engine)

    def _16_query_betting_members(self) -> pd.DataFrame:
        """查询投注金额大于10000的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.valid_bet_amount) > 10000
        """
        return pd.read_sql(query, self.engine)

    def _17_query_high_profit_members(self) -> pd.DataFrame:
        """查询输钱或赢钱大于3000的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.profit) > 3000 OR SUM(b_mds.profit) < -3000
        """
        return pd.read_sql(query, self.engine)

    def _18_query_frequent_depositors(self) -> pd.DataFrame:
        """查询存款次数大于3的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.draw_count) > 3
        """
        return pd.read_sql(query, self.engine)

    def _19_query_high_depositors(self) -> pd.DataFrame:
        """查询存款金额大于5000的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.draw) > 5000
        """
        return pd.read_sql(query, self.engine)

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
    result_df = (db_query._10_query_promotion()
                 .merge(db_query._1_query_member_stats(), on='会员ID', how='inner')
                 )
    return result_df

def main():
    start_time = datetime.now()
    print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")
    db_query = DatabaseQuery(
        host='18.178.159.230',port=3366,user='bigdata',password='uvb5SOSmLH8sCoSU',)
    try:
        result = work(db_query)
        excel_filename = f"query_result_{start_time.strftime('%Y-%m-%d_%H.%M')}.xlsx"
        save_to_excel(result, excel_filename)
        print(f"结果已保存到: {excel_filename}")
    finally:
        db_query.close_session()
        end_time = datetime.now()
        print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"总运行时间: {str(end_time - start_time).split('.')[0]}")


if __name__ == "__main__":
    main()