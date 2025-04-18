import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymongo
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
import os


def execute_mongo_aggregation(collection_name: str, pipeline: list, mongo_uri: str, db_name: str) -> pd.DataFrame:
    """执行 MongoDB 聚合查询的通用方法"""
    client = pymongo.MongoClient(mongo_uri)
    collection = client[db_name][collection_name]
    try:
        df = pd.DataFrame(list(collection.aggregate(pipeline, cursor={}, batchSize=5000)))
    except pymongo.errors.PyMongoError as e:
        print(f"MongoDB query failed for {collection_name}: {e}")
        return pd.DataFrame()
    finally:
        client.close()
    return df


class DatabaseQuery:
    def __init__(self, host: str, port: int, user: str, password: str,
                 mongo_host: str, mongo_port: int, mongo_user: str, mongo_password: str,
                 site_id: int = 1000, start_date: str = '2025-03-31', end_date: str = '2025-03-31',
                 agent_1000: str = 'agent_1000', u1_1000: str = 'u1_1000',
                 bigdata: str = 'bigdata', control_1000: str = 'control_1000',
                 mongo_collection_prefix: str = 'pull_order_game_', venue: str = 'DBDJ'):
        """初始化数据库连接参数"""
        # MySQL 连接
        connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/"
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        # MongoDB 连接参数
        self.mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/"
        self.mongo_db_name = "update_records"
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db_name]
        self.mongo_collection_prefix = mongo_collection_prefix
        self.venue = venue
        self.batch_size = 5000
        self.flag_value = 1

        # 其他参数
        self.control_1000 = control_1000
        self.bigdata = bigdata
        self.agent_1000 = agent_1000
        self.u1_1000 = u1_1000
        self.site_id = site_id
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = f"{start_date} 00:00:00"
        self.end_time = f"{end_date} 23:59:59"

    def _process_mongo_collections(self, collections: list, pipeline: list) -> pd.DataFrame:
        """使用多进程处理 MongoDB **的通用方法"""
        processes = min(4, os.cpu_count() or 1)
        with Pool(processes=processes) as pool:
            partial_process = partial(
                execute_mongo_aggregation,
                pipeline=pipeline,
                mongo_uri=self.mongo_uri,
                db_name=self.mongo_db_name
            )
            results = list(
                tqdm(pool.imap(partial_process, collections), total=len(collections), desc="Processing collections"))
        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    def close_connections(self):
        """关闭 MySQL 和 MongoDB 连接"""
        if self.session:
            self.session.close()
        self.client.close()

    def Custom(self) -> pd.DataFrame:
        """查询登入注会员"""
        query = """
        """
        return pd.read_sql(query, self.engine)

    def _1_member_stats(self) -> pd.DataFrame:
        """查询会员历史累计统计信息"""
        query = f"""
        SELECT
            u1_mi.id AS '会员ID',
            u1_mi.name AS '会员账号',
            CASE u1_mi.status
                WHEN 1 THEN '启用'
                WHEN 0 THEN '禁用'
                ELSE CAST(u1_mi.status AS CHAR)
            END AS '状态',
            u1_mi.is_agent AS '是否代理',            
            u1_mi.vip_grade AS 'VIP等级',
            GROUP_CONCAT(c1_sv.dict_value ORDER BY c1_sv.code SEPARATOR ',') AS '标签',
            u1_mi.created_at AS '注册时间',
            u1_mi.last_login_time AS '最后登录时间',
            COALESCE(SUM(b_mds.deposit_count), 0) AS '历史存款笔数',
            COALESCE(SUM(b_mds.deposit), 0) AS '历史存款',
            COALESCE(SUM(b_mds.draw_count), 0) AS '历史取款笔数',
            COALESCE(SUM(b_mds.draw), 0) AS '历史取款',
            COALESCE(SUM(b_mds.bets), 0) AS '历史有效投注金额',
            COALESCE(-SUM(b_mds.profit), 0) AS '历史总输赢',
            COALESCE(SUM(b_mds.promo), 0) AS '历史红利',
            COALESCE(SUM(b_mds.rebate), 0) AS '历史返水'
        FROM {self.u1_1000}.member_info u1_mi
        LEFT JOIN {self.control_1000}.sys_dict_value c1_sv
            ON FIND_IN_SET(c1_sv.code, u1_mi.tag_id)
        LEFT JOIN {self.bigdata}.member_daily_statics b_mds
            ON u1_mi.id = b_mds.member_id
        WHERE u1_mi.status <> 0
            AND (c1_sv.initial_flag IS NULL OR c1_sv.initial_flag <> 1)
        GROUP BY u1_mi.id,u1_mi.name,u1_mi.vip_grade,u1_mi.created_at,u1_mi.last_login_time
        """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _10_promotion(self) -> pd.DataFrame:
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
        LEFT JOIN {self.u1_1000}.member_info u1_mi
            ON u1_mi.top_name = a1_adm.agent_name
        WHERE
            a1_ad.group_name = '推广部'
            AND a1_ad_2.group_name IN ('推广1部', '推广3部', '推广5部', '推广6部', '推广7部', '推广9部', '推广11部')
            AND a1_ad.site_id = {self.site_id}
        """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _11_login_members(self) -> pd.DataFrame:
        """查询登入注会员"""
        query = f"""
        SELECT DISTINCT u1_mi.id AS '会员ID'
        FROM {self.u1_1000}.member_info u1_mi
        WHERE u1_mi.last_login_time > '{self.start_date}'
        AND u1_mi.site_id = {self.site_id}
        """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _12_active_low_depositors(self) -> pd.DataFrame:
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
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _14_recent_login_members(self) -> pd.DataFrame:
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
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _15_merge_login_non_betting_members(self) -> pd.DataFrame:
        """使用 SQL INNER JOIN 合并登录会员和 antecede会员查询，获取同时满足条件的会员ID"""
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
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _16_bería_members(self) -> pd.DataFrame:
        """查询投注金额大于10000的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.valid_bet_amount) > 10000
        """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _17_high_profit_members(self) -> pd.DataFrame:
        """查询输钱或赢钱大于3000的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.profit) > 3000 OR SUM(b_mds.profit) < -3000
        """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _18_frequent_depositors(self) -> pd.DataFrame:
        """查询存款次数大于3的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.draw_count) > 3
        """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _19_high_depositors(self) -> pd.DataFrame:
        """查询存款金额大于5000的会员"""
        query = f"""
        SELECT b_mds.member_id AS '会员ID'
        FROM {self.bigdata}.member_daily_statics b_mds
        WHERE b_mds.statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        AND b_mds.site_id = {self.site_id}
        GROUP BY b_mds.member_id
        HAVING SUM(b_mds.draw) > 5000
        """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def mongo_last_bet_time(self) -> pd.DataFrame:
        """查询每个会员的最后下注时间"""
        collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
        if not collections:
            return pd.DataFrame(columns=['会员ID', '最后下注时间'])

        pipeline = [
            {"$match": {
                "flag": self.flag_value,
                "settle_time": {"$gte": self.start_time, "$lte": self.end_time},
                "site_id": self.site_id
            }},
            {"$group": {
                "_id": "$member_id",
                "last_bet_time": {"$max": "$bet_time"}
            }},
            {"$project": {
                "_id": 0,
                "会员ID": "$_id",
                "最后下注时间": "$last_bet_time"
            }}
        ]

        df = self._process_mongo_collections(collections, pipeline)
        return df.astype({'会员ID': 'category'}) if not df.empty else pd.DataFrame(columns=['会员ID', '最后下注时间'])

    def mongo_betting_stats(self) -> pd.DataFrame:
        """查询 MongoDB 投注统计数据"""
        collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
        if not collections:
            return pd.DataFrame(columns=['会员ID', '投注次数', '有效投注', '会员输赢'])

        pipeline = [
            {"$match": {
                "flag": 1,
                "settle_time": {"$gte": self.start_time, "$lte": self.end_time},
                "site_id": self.site_id
            }},
            {"$group": {
                "_id": {"member_id": "$member_id", "game_type": "$game_type"},
                "betting_count": {"$sum": 1},
                "valid_bet": {"$sum": "$valid_bet_amount"},
                "net_amount": {"$sum": "$net_amount"}
            }},
            {"$project": {
                "_id": 0,
                "会员ID": "$_id.member_id",
                "game_type": "$_id.game_type",
                "betting_count": 1,
                "valid_bet": 1,
                "net_amount": 1
            }}
        ]

        df = self._process_mongo_collections(collections, pipeline)
        if df.empty:
            return pd.DataFrame(columns=['会员ID', '投注次数', '有效投注', '会员输赢'])

        df = df.astype({'会员ID': 'category', 'game_type': 'int8', 'betting_count': 'int32', 'valid_bet': 'float32',
                        'net_amount': 'float32'})

        # 聚合会员统计
        member_stats = df.groupby('会员ID', observed=True).agg({
            'betting_count': 'sum',
            'valid_bet': 'sum',
            'net_amount': 'sum'
        }).reset_index().rename(columns={
            'betting_count': '投注次数',
            'valid_bet': '有效投注',
            'net_amount': '会员输赢'
        })

        # 游戏类型映射
        game_types = {
            1: ('体育有效投注', '体育会员输赢'),
            2: ('电竞有效投注', '电竞会员输赢'),
            3: ('真人有效投注', '真人会员输赢'),
            4: ('彩票有效投注', '彩票会员输赢'),
            5: ('棋牌有效投注', '棋牌会员输赢'),
            6: ('电子有效投注', '电子会员输赢'),
            7: ('捕鱼有效投注', '捕鱼会员输赢')
        }

        # 有效投注和输赢透视表
        valid_pivot = df.pivot_table(index='会员ID', columns='game_type', values='valid_bet', aggfunc='sum',
                                     fill_value=0)
        valid_pivot.columns = [game_types.get(col, (str(col),))[0] for col in valid_pivot.columns]
        net_pivot = df.pivot_table(index='会员ID', columns='game_type', values='net_amount', aggfunc='sum',
                                   fill_value=0)
        net_pivot.columns = [game_types.get(col, (str(col),))[1] for col in net_pivot.columns]

        # 合并结果
        result = member_stats.merge(valid_pivot, on='会员ID', how='outer').merge(net_pivot, on='会员ID', how='outer')
        return result[result['有效投注'] > 0]

    def mongo_betting_details(self) -> pd.DataFrame:
        """查询 MongoDB 投注详细记录"""
        collections = [col for col in self.db.list_collection_names() if
                       col.startswith(self.mongo_collection_prefix) and col.endswith(self.venue)]
        if not collections:
            return pd.DataFrame(columns=[
                '会员ID', '结算日期', '会员账号', '场馆名称', '游戏名称', '赛事ID', '注单号',
                '赔率类型', '赔率', '投注额', '有效投注', '会员输赢', '是否提前结算',
                '投注时间', '开始时间', '结算时间', '游戏详情'
            ])

        pipeline = [
            {"$match": {
                "flag": self.flag_value,
                "settle_time": {"$gte": self.start_time, "$lte": self.end_time},
                "site_id": self.site_id
            }},
            {"$sort": {"bet_time": 1}},
            {"$project": {
                "_id": 0,
                "会员ID": "$member_id",
                "结算日期": {"$substr": ["$settle_time", 0, 10]},
                "会员账号": "$member_name",
                "场馆名称": "$venue_name",
                "游戏名称": "$game_name",
                "赛事ID": "$match_id",
                "注单号": "$id",
                "赔率类型": "$odds_type",
                "赔率": "$odds",
                "投注额": "$bet_amount",
                "有效投注": "$valid_bet_amount",
                "会员输赢": "$net_amount",
                "是否提前结算": "$early_settle_flag",
                "投注时间": "$bet_time",
                "开始时间": "$start_time",
                "结算时间": "$settle_time",
                "游戏详情": "$play_info"
            }}
        ]

        df = self._process_mongo_collections(collections, pipeline)
        return df.astype({
            '会员ID': 'category',
            '投注额': 'float32',
            '有效投注': 'float32',
            '会员输赢': 'float32',
            '赔率': 'float32'
        }) if not df.empty else pd.DataFrame(columns=[
            '会员ID', '结算日期', '会员账号', '场馆名称', '游戏名称', '赛事ID', '注单号',
            '赔率类型', '赔率', '投注额', '有效投注', '会员输赢', '是否提前结算',
            '投注时间', '开始时间', '结算时间', '游戏详情'
        ])


def save_to_excel(df: pd.DataFrame, filename: str):
    """保存 DataFrame 到 Excel 文件"""
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        df.drop(columns=['member_id'], errors='ignore').to_excel(writer, sheet_name='Sheet1', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        worksheet.freeze_panes(1, 0)
        worksheet.autofilter(0, 0, 0, len(df.columns) - 1)


def work(db_query: DatabaseQuery) -> pd.DataFrame:
    """执行查询并合并结果"""
    result_df = (db_query._10_promotion()
                 .merge(db_query._1_member_stats(), on='会员ID', how='inner')
                 .merge(db_query.mongo_last_bet_time(), on='会员ID', how='inner')
                 .merge(db_query.mongo_betting_stats(), on='会员ID', how='inner')
                 )
    return result_df


def main():
    start_time = datetime.now()
    print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")
    db_query = DatabaseQuery(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        mongo_host='18.178.159.230',
        mongo_port=27217,
        mongo_user='biddata',
        mongo_password='uvb5SOSmLH8sCoSU'
    )
    try:
        result = work(db_query)
        excel_filename = f"Ultimate_{start_time.strftime('%Y-%m-%d_%H.%M')}.xlsx"
        save_to_excel(result, excel_filename)
        print(f"结果已保存到: {excel_filename}")
    except Exception as e:
        print(f"运行失败: {e}")
    finally:
        db_query.close_connections()
        end_time = datetime.now()
        print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"总运行时间: {str(end_time - start_time).split('.')[0]}")


if __name__ == "__main__":
    main()

