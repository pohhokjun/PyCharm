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
import requests
import traceback
import re


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
                 site_id: int = 1000, start_date: str = '2025-04-01', end_date: str = '2025-04-30',
                 agent_1000: str = 'agent_1000', u1_1000: str = 'u1_1000',
                 bigdata: str = 'bigdata', control_1000: str = 'control_1000',
                 finance_1000: str = 'finance_1000',
                 mongo_collection_prefix: str = 'pull_order_game_', venue: str = ''):
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
        self.finance_1000 = finance_1000
        self.site_id = site_id
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = f"{start_date} 00:00:00"
        self.end_time = f"{end_date} 23:59:59"

    def _process_mongo_collections(self, collections: list, pipeline: list) -> pd.DataFrame:
        """使用多进程处理 MongoDB 的通用方法"""
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

    def _0_promotion(self) -> pd.DataFrame:
        """查询推广部相关信息"""
        query = f"""
       SELECT
           a1_ad.group_name AS '1级',
           a1_ad_2.group_name AS '2级',
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
           a1_ad.level = 1
       """
        # AND a1_ad.group_name = '推广部'
        # AND a1_ad_2.group_name IN ('推广1部', '推广3部', '推广5部', '推广6部', '推广7部', '推广9部', '推广11部')
        # AND a1_adm.agent_name IN ('qq7236345', 's2009s', 'wang1246141')
        # AND u1_mi.id IN ('qq7236345', 's2009s', 'wang1246141')
        if self.site_id is not None:
            query += f" AND a1_ad.site_id = {self.site_id}"
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _1_member_basic_info(self) -> pd.DataFrame:
        """查询会员基本信息"""
        query = f"""
        SELECT
           u1_mi.site_id AS '站点ID',
           u1_mi.top_name AS '代理名称',
           u1_mi.id AS '会员ID',
           u1_mi.name AS '会员账号',
           CASE u1_mi.status WHEN 1 THEN '启用' WHEN 0 THEN '禁用' ELSE CAST(u1_mi.status AS CHAR) END AS '状态',
           u1_mi.vip_grade AS 'VIP等级',
           (SELECT GROUP_CONCAT(DISTINCT c1_sv.dict_value ORDER BY c1_sv.code SEPARATOR ',')
            FROM control_1000.sys_dict_value c1_sv
            WHERE FIND_IN_SET(c1_sv.code, u1_mi.tag_id)
            AND (c1_sv.initial_flag IS NULL OR c1_sv.initial_flag <> 1)) AS '标签',
           u1_mofr.remark AS '备注'
        FROM u1_1000.member_info u1_mi
        LEFT JOIN (
           SELECT member_id, remark
           FROM (
               SELECT member_id, remark,
                      ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY updated_at DESC) AS rn
               FROM u1_1000.member_open_forbid_record
               WHERE remark_type = 1
           ) t
           WHERE t.rn = 1
        ) u1_mofr ON u1_mi.id = u1_mofr.member_id
        LEFT JOIN (
           SELECT DISTINCT member_info_id
           FROM u1_1000.member_banks_info
        ) u1_mbi ON u1_mi.id = u1_mbi.member_info_id
        """
        # WHERE u1_mi.name IN ('qq7236345', 's2009s', 'wang1246141')
        if self.site_id is not None:
            query += f" WHERE u1_mi.site_id = {self.site_id}"
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _2_first_deposit(self) -> pd.DataFrame:
        """查询会员首存信息"""
        query = f"""
        SELECT
            member_id AS '会员ID',
            order_amount AS '首存金额'
        FROM (
            SELECT
                member_id,
                order_amount,
                confirm_at,
                ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY confirm_at ASC) AS rn
            FROM {self.finance_1000}.finance_pay_records
            WHERE is_first_deposit = 1
        ) t
        WHERE rn = 1
        And order_amount > 0
       """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _3_last_bet_date(self) -> pd.DataFrame:
        """查询会员最后投注日期信息"""
        query = f"""
        SELECT 
            member_id AS '会员ID',
            MAX(CASE WHEN valid_bet_amount > 0 THEN statics_date ELSE NULL END) AS '最后投注日期'
        FROM {self.bigdata}.member_daily_statics
        GROUP BY member_id
       """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _6_member_stats_period(self, use_date_column: bool = False) -> pd.DataFrame:
        """查询会员指定时间段的统计信息 True False """
        if use_date_column:
            query = f"""
           SELECT
               statics_date AS '日期',
               member_id AS '会员ID',
               COALESCE(SUM(deposit_count), 0) AS '存款笔数',
               COALESCE(SUM(deposit), 0) AS '存款',
               COALESCE(SUM(draw_count), 0) AS '取款笔数',
               COALESCE(SUM(draw), 0) AS '取款',
               COALESCE(SUM(bets), 0) AS '有效投注金额',
               COALESCE(-SUM(profit), 0) AS '会员输赢',
               COALESCE(SUM(promo), 0) AS '红利',
               COALESCE(SUM(rebate), 0) AS '返水'
           FROM {self.bigdata}.member_daily_statics
           WHERE statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
           GROUP BY member_id, statics_date
           """
        else:
            query = f"""
            SELECT
               member_id AS '会员ID',
               COALESCE(SUM(deposit), 0) AS '存款',
               COALESCE(SUM(draw), 0) AS '取款',
               COALESCE(SUM(bets), 0) AS '有效投注金额',
               COALESCE(SUM(profit), 0) AS '公司输赢',
               COALESCE(SUM(promo), 0) AS '红利',
               COALESCE(SUM(rebate), 0) AS '返水',
               COALESCE(SUM(profit), 0)/COALESCE(SUM(all_bets), 0) AS '盈余比例'
           FROM bigdata.member_daily_statics
           WHERE statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
           GROUP BY member_id
           HAVING COALESCE(SUM(bets), 0) > 0
           """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)


    def mongo_betting_stats(self, use_date_column: bool = False) -> pd.DataFrame:
        """查询 MongoDB 投注统计数据 True False """
        collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
        if not collections:
            return pd.DataFrame(
                columns=['日期', '会员ID', '投注次数', '有效投注', '会员输赢', '会员投注喜好'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢','会员投注喜好'])

        pipeline = [
            {"$match": {
                "flag": 1,
                "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
            }},
            {"$sort": {"settle_time": 1}},
            {"$group": {
                "_id": {
                    "member_id": "$member_id",
                    "game_type": "$game_type",
                    "date": {"$dateToString": {"format": "%Y-%m-%d",
                                               "date": {"$toDate": "$settle_time"}}} if use_date_column else None
                },
                "betting_count": {"$sum": 1},
                "valid_bet": {"$sum": "$valid_bet_amount"},
                "net_amount": {"$sum": "$net_amount"}
            }},
            {"$project": {
                "_id": 0,
                "日期": "$_id.date" if use_date_column else None,
                "会员ID": "$_id.member_id",
                "game_type": "$_id.game_type",
                "betting_count": 1,
                "valid_bet": 1,
                "net_amount": 1
            }}
        ]
        if self.site_id is not None:
            pipeline[0]["$match"]["site_id"] = self.site_id
        # 移除 None 值
        pipeline = [{k: v for k, v in stage.items() if v is not None} for stage in pipeline]

        df = self._process_mongo_collections(collections, pipeline)
        if df.empty:
            return pd.DataFrame(
                columns=['日期', '会员ID', '投注次数', '有效投注', '会员输赢', '会员投注喜好'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢','会员投注喜好'])

        df = df.astype({'会员ID': 'category', 'game_type': 'int8', 'betting_count': 'int32',
                        'valid_bet': 'float32', 'net_amount': 'float32'})
        if use_date_column:
            df['日期'] = df['日期'].astype('string')

        # 聚合会员统计
        group_cols = ['日期', '会员ID'] if use_date_column else ['会员ID']
        member_stats = df.groupby(group_cols, observed=True).agg({
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
        pivot_index = ['日期', '会员ID'] if use_date_column else ['会员ID']
        valid_pivot = df.pivot_table(index=pivot_index, columns='game_type', values='valid_bet',
                                     aggfunc='sum', fill_value=0, observed=False).reset_index()
        valid_pivot.columns = pivot_index + [game_types.get(col, (str(col),))[0] for col in
                                             valid_pivot.columns[len(pivot_index):]]
        net_pivot = df.pivot_table(index=pivot_index, columns='game_type', values='net_amount',
                                   aggfunc='sum', fill_value=0, observed=False).reset_index()
        net_pivot.columns = pivot_index + [game_types.get(col, (str(col),))[1] for col in
                                           net_pivot.columns[len(pivot_index):]]

        # 计算会员投注喜好
        game_valid_cols = [col for col in valid_pivot.columns if col.endswith('有效投注')]
        if game_valid_cols: # 确保有游戏有效投注列
            valid_pivot['max_valid_bet_value'] = valid_pivot[game_valid_cols].max(axis=1)
            valid_pivot['会员投注喜好'] = valid_pivot[game_valid_cols].idxmax(axis=1)
            # 如果最大有效投注为0，则喜好为'无'
            valid_pivot['会员投注喜好'] = valid_pivot.apply(
                lambda row: row['会员投注喜好'] if row['max_valid_bet_value'] > 0 else '无', axis=1
            )
            valid_pivot = valid_pivot.drop(columns=['max_valid_bet_value'])
        else:
             valid_pivot['会员投注喜好'] = '无' # 如果没有游戏有效投注列，则喜好为'无'


        # 修改排序：按场馆顺序合并有效投注和会员输赢
        result = member_stats
        # 合并投注喜好
        result = result.merge(valid_pivot[pivot_index + ['会员投注喜好']], on=pivot_index, how='left')

        for game_type in sorted(game_types.keys()):
            valid_col = game_types[game_type][0]
            net_col = game_types[game_type][1]
            if valid_col in valid_pivot.columns:
                result = result.merge(valid_pivot[pivot_index + [valid_col]], on=pivot_index, how='outer')
            if net_col in net_pivot.columns:
                result = result.merge(net_pivot[pivot_index + [net_col]], on=pivot_index, how='outer')


        # 确保最终列顺序
        final_columns_base = ['日期', '会员ID', '投注次数', '有效投注', '会员输赢'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢']
        final_columns = final_columns_base + ['会员投注喜好'] + \
                        [col for col in result.columns if col not in (final_columns_base + ['会员投注喜好'])]
        result = result.reindex(columns=final_columns)

        return result[result['有效投注'] > 0]

    def mongo_betting_venue_column(self, use_date_column: bool = False) -> pd.DataFrame:
        """查询 MongoDB 投注统计数据，按 venue 分组并透视"""
        collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
        if not collections:
            # 返回一个空的 DataFrame，包含可能的索引列
            return pd.DataFrame(columns=['日期', '会员ID'] if use_date_column else ['会员ID'])

        # 构建聚合管道
        pipeline = [
            {"$match": {
                "flag": 1,
                "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
            }},
            {"$sort": {"settle_time": 1}},
            {"$group": {
                "_id": {
                    "member_id": "$member_id",
                    "date": {"$dateToString": {"format": "%Y-%m-%d",
                                               "date": {"$toDate": "$settle_time"}}} if use_date_column else None,
                    "venue": "$venue_name"  # 假设 MongoDB 文档有 venue_name 字段
                },
                # 保留有效投注和会员输赢
                "valid_bet": {"$sum": "$valid_bet_amount"},
                "net_amount": {"$sum": "$net_amount"}
            }},
            {"$project": {
                "_id": 0,
                "日期": "$_id.date" if use_date_column else None,
                "会员ID": "$_id.member_id",
                "场馆": "$_id.venue",
                "valid_bet": 1,
                "net_amount": 1
            }}
        ]
        if self.site_id is not None:
            pipeline[0]["$match"]["site_id"] = self.site_id
        # 移除 None 值
        pipeline = [{k: v for k, v in stage.items() if v is not None} for stage in pipeline]

        # 执行聚合查询
        raw_df = self._process_mongo_collections(collections, pipeline)

        if raw_df.empty:
            return pd.DataFrame(columns=['日期', '会员ID'] if use_date_column else ['会员ID'])

        # 类型优化
        raw_df = raw_df.astype({'会员ID': 'category', '场馆': 'string',
                                'valid_bet': 'float32', 'net_amount': 'float32'})
        if use_date_column:
            raw_df['日期'] = raw_df['日期'].astype('string')

        # 定义场馆名称映射
        venue_mapping = {
            "CQ9ZR": "MT真人", "YXZR": "亚星真人", "EVOZR": "EVO真人", "SGCP": "双赢彩票",
            "CQ9BY": "MT弹珠", "CQ9DZ": "CQ9电子", "CRTY": "皇冠体育", "AGBY": "AG捕鱼",
            "PTDZ": "PT电子", "LHDJ": "雷火电竞", "PPZR": "PP真人", "IMTY": "IM体育",
            "DBBY": "DB捕鱼", "TCGCP": "TCG彩票", "IMDJ": "IM电竞", "BBINZR": "BBIN真人",
            "BYBY": "博雅捕鱼", "BYQP": "博雅棋牌", "BGZR": "BG真人", "EGDZ": "EG电子",
            "SBTY": "沙巴体育", "WMZR": "完美真人", "DBGGL": "DB刮刮乐", "GFQP": "GFG棋牌",
            "FBTY": "FB体育", "DBDJ": "多宝电竞", "DBQP": "多宝棋牌", "DBHX": "多宝哈希",
            "IMQP": "IM棋牌", "XMTY": "熊猫体育", "AGDZ": "AG电子", "DBDZ": "多宝电子",
            "AGZR": "AG真人", "DBZR": "多宝真人", "DBCP": "多宝彩票", "GFDZ": "GFG电子",
            "PPDZ": "PP电子", "PGDZ": "PG电子"
        }

        # 应用场馆名称映射
        raw_df.loc[:, '场馆'] = raw_df['场馆'].map(venue_mapping).fillna(raw_df['场馆'])

        # 定义透视表的索引列
        pivot_index = ['日期', '会员ID'] if use_date_column else ['会员ID']

        # 透视 DataFrame
        pivot_df = raw_df.pivot_table(
            index=pivot_index,
            columns='场馆',
            values=['valid_bet', 'net_amount'],  # 使用原始字段名进行透视
            aggfunc='sum',
            fill_value=0
        )

        metric_mapping = {
            'valid_bet': '有效投注',
            'net_amount': '会员输赢'
        }
        # 创建新的列名，格式为 "场馆名称指标名称"
        new_columns = [f'{col[1]}{metric_mapping[col[0]]}' for col in pivot_df.columns]
        pivot_df.columns = new_columns

        # 重置索引
        result = pivot_df.reset_index()

        return result

    def mongo_betting_venue_ranking(self) -> pd.DataFrame:
        """查询 MongoDB 投注统计数据，按 venue 分组并计算场馆排名"""
        collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
        if not collections:
            # 返回一个空的 DataFrame，包含所有期望的列
            return pd.DataFrame(columns=['会员ID', '投注前1场馆', '投注前2场馆', '投注前3场馆',
                                         '公司输赢前1场馆', '公司输赢前2场馆', '公司输赢前3场馆',
                                         '公司输赢后1场馆', '公司输赢后2场馆', '公司输赢后3场馆'])

        # 构建聚合管道：按会员ID和场馆分组，计算有效投注总额和会员输赢总额
        pipeline = [
            {"$match": {
                "flag": 1,
                "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
            }},
            {"$sort": {"settle_time": 1}},  # 排序通常不是必须的，但保留原结构
            {"$group": {
                "_id": {
                    "member_id": "$member_id",
                    "venue": "$venue_name"  # 假设 MongoDB 文档有 venue_name 字段
                },
                "total_valid_bet": {"$sum": "$valid_bet_amount"},
                "total_net_amount": {"$sum": "$net_amount"}  # 会员输赢
            }},
            {"$project": {
                "_id": 0,
                "会员ID": "$_id.member_id",
                "场馆": "$_id.venue",
                "total_valid_bet": 1,
                "total_net_amount": 1  # 会员输赢
            }}
        ]
        if self.site_id is not None:
            pipeline[0]["$match"]["site_id"] = self.site_id

        raw_df = self._process_mongo_collections(collections, pipeline)

        if raw_df.empty:
            return pd.DataFrame(columns=['会员ID', '投注前1场馆', '投注前2场馆', '投注前3场馆',
                                         '公司输赢前1场馆', '公司输赢前2场馆', '公司输赢前3场馆',
                                         '公司输赢后1场馆', '公司输赢后2场馆', '公司输赢后3场馆'])

        # 类型优化
        raw_df = raw_df.astype({'会员ID': 'category', '场馆': 'string',
                                'total_valid_bet': 'float32', 'total_net_amount': 'float32'})

        # 定义场馆名称映射 (保留原有的映射)
        venue_mapping = {
            "CQ9ZR": "MT真人", "YXZR": "亚星真人", "EVOZR": "EVO真人", "SGCP": "双赢彩票",
            "CQ9BY": "MT弹珠", "CQ9DZ": "CQ9电子", "CRTY": "皇冠体育", "AGBY": "AG捕鱼",
            "PTDZ": "PT电子", "LHDJ": "雷火电竞", "PPZR": "PP真人", "IMTY": "IM体育",
            "DBBY": "DB捕鱼", "TCGCP": "TCG彩票", "IMDJ": "IM电竞", "BBINZR": "BBIN真人",
            "BYBY": "博雅捕鱼", "BYQP": "博雅棋牌", "BGZR": "BG真人", "EGDZ": "EG电子",
            "SBTY": "沙巴体育", "WMZR": "完美真人", "DBGGL": "DB刮刮乐", "GFQP": "GFG棋牌",
            "FBTY": "FB体育", "DBDJ": "多宝电竞", "DBQP": "多宝棋牌", "DBHX": "多宝哈希",
            "IMQP": "IM棋牌", "XMTY": "熊猫体育", "AGDZ": "AG电子", "DBDZ": "多宝电子",
            "AGZR": "AG真人", "DBZR": "多宝真人", "DBCP": "多宝彩票", "GFDZ": "GFG电子",
            "PPDZ": "PP电子", "PGDZ": "PG电子"
        }
        # 应用场馆名称映射
        raw_df.loc[:, '场馆'] = raw_df['场馆'].map(venue_mapping).fillna(raw_df['场馆'])

        # 计算公司输赢 (公司输赢 = -会员输赢)
        raw_df['total_company_win'] = -raw_df['total_net_amount']

        # 为每个会员计算场馆排名
        def get_ranked_venues(group):
            # 投注排名 (有效投注从高到低)
            bet_ranked = group.sort_values(by='total_valid_bet', ascending=False).head(3)['场馆'].tolist()
            # 公司输赢排名 (公司输赢从高到低，即会员输赢从低到高)
            company_win_ranked_top = group.sort_values(by='total_company_win', ascending=False).head(3)['场馆'].tolist()
            # 公司输赢排名 (公司输赢从低到高，即会员输赢从高到低)
            company_win_ranked_bottom = group.sort_values(by='total_company_win', ascending=True).head(3)[
                '场馆'].tolist()

            # 填充不足3个场馆的情况
            bet_ranked += [''] * (3 - len(bet_ranked))
            company_win_ranked_top += [''] * (3 - len(company_win_ranked_top))
            company_win_ranked_bottom += [''] * (3 - len(company_win_ranked_bottom))

            return pd.Series({
                '投注前1场馆': bet_ranked[0],
                '投注前2场馆': bet_ranked[1],
                '投注前3场馆': bet_ranked[2],
                '公司输赢前1场馆': company_win_ranked_top[0],
                '公司输赢前2场馆': company_win_ranked_top[1],
                '公司输赢前3场馆': company_win_ranked_top[2],
                '公司输赢后1场馆': company_win_ranked_bottom[0],
                '公司输赢后2场馆': company_win_ranked_bottom[1],
                '公司输赢后3场馆': company_win_ranked_bottom[2],
            })

        # 按会员ID分组并应用排名函数
        ranked_venues_df = raw_df.groupby('会员ID', observed=True).apply(get_ranked_venues).reset_index()

        return ranked_venues_df


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
    # 推广架构 db_query._0_promotion()
    # 会员信息 .merge(db_query._1_member_basic_info(), on='会员ID', how='inner')
    # 首存 .merge(db_query._2_first_deposit(), on='会员ID', how='left')
    # 最后投注 .merge(db_query._3_last_bet_date(), on='会员ID', how='left')
    # 会员数据（时间段） .merge(db_query._6_member_stats_period(use_date_column=True False), on='会员ID', how='inner')
    # 会员数据（历史） .merge(db_query._7_member_stats_history(), on='会员ID', how='inner')
    # 游戏 .merge(db_query.mongo_betting_stats(use_date_column=True False), on=['会员ID', '日期'], how='inner')
    result_df = (db_query._1_member_basic_info()
                 .merge(db_query._2_first_deposit(), on='会员ID', how='inner')
                 .merge(db_query._3_last_bet_date(), on='会员ID', how='left')
                 .merge(db_query._0_promotion(), on='会员ID', how='left')
                 .merge(db_query._6_member_stats_period(use_date_column=False), on='会员ID', how='inner')
                 .merge(db_query.mongo_betting_stats(use_date_column=False), on=['会员ID'], how='left')
                 .merge(db_query.mongo_betting_venue_column(use_date_column=False), on=['会员ID'], how='left')
                 .merge(db_query.mongo_betting_venue_ranking(), on='会员ID', how='left')
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
        if db_query.start_date == db_query.end_date:
            date_str = datetime.strptime(db_query.start_date, '%Y-%m-%d').strftime('%#m-%#d')
        else:
            date_str = f"{datetime.strptime(db_query.start_date, '%Y-%m-%d').strftime('%#m-%#d')}-{datetime.strptime(db_query.end_date, '%Y-%m-%d').strftime('%#m-%#d')}"
        excel_filename = f"【{db_query.site_id if db_query.site_id is not None else 'ALL'}_{date_str}_{db_query.venue}】{start_time.strftime('%#m-%#d_%H.%M')}.xlsx"
        save_to_excel(result, excel_filename)
        print(f"结果已保存到: {excel_filename}")
        # send_to_telegram(excel_filename, TELEGRAM_BOT_TOKEN, CHAT_ID)
    except Exception as e:
        print(f"运行失败: {e}")
        traceback.print_exc()
    finally:
        db_query.close_connections()
        end_time = datetime.now()
        print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"总运行时间: {str(end_time - start_time).split('.')[0]}")


if __name__ == "__main__":
    main()
