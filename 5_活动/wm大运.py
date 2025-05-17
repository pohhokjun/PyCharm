
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
                 site_id: int = 1000, start_date: str = '2025-04-16', end_date: str = '2025-04-29',
                 agent_1000: str = 'agent_1000', u1_1000: str = 'u1_1000',
                 bigdata: str = 'bigdata', control_1000: str = 'control_1000',
                 finance_1000: str = 'finance_1000',
                 mongo_collection_prefix: str = 'pull_order_game_', venue: str = 'GFQP'):
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

    def _1_member_basic_info(self) -> pd.DataFrame:
        """查询会员基本信息"""
        query = f"""
        SELECT
           u1_mi.site_id AS '站点ID',
           u1_mi.top_name AS '代理名称',
           u1_mi.id AS '会员ID',
           u1_mi.name AS '会员账号',
           u1_mi.vip_grade AS 'VIP等级',
           (SELECT GROUP_CONCAT(DISTINCT c1_sv.dict_value ORDER BY c1_sv.code SEPARATOR ',')
            FROM control_1000.sys_dict_value c1_sv
            WHERE FIND_IN_SET(c1_sv.code, u1_mi.tag_id)
            AND (c1_sv.initial_flag IS NULL OR c1_sv.initial_flag <> 1)) AS '标签',
           u1_mofr.remark AS '备注',
           u1_mi.created_at AS '注册时间',
           u1_mi.last_login_time AS '最后登录时间'
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
        """
        if self.site_id is not None:
            query += f" WHERE u1_mi.site_id = {self.site_id}"
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
           FROM {self.bigdata}.member_daily_statics
           WHERE statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
           GROUP BY member_id, statics_date
           """
        else:
            query = f"""
           SELECT
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
           GROUP BY member_id
           """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def mongo_betting_stats(self, use_date_column: bool = False) -> pd.DataFrame:
        """查询 MongoDB 投注统计数据 True False """
        collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
        if not collections:
            return pd.DataFrame(
                columns=['日期', '会员ID', '投注次数', '有效投注', '会员输赢'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢'])

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
        pipeline = [{k: v for k, v in stage.items() if v is not None} for stage in pipeline]

        df = self._process_mongo_collections(collections, pipeline)
        if df.empty:
            return pd.DataFrame(
                columns=['日期', '会员ID', '投注次数', '有效投注', '会员输赢'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢'])

        df = df.astype({'会员ID': 'category', 'game_type': 'int8', 'betting_count': 'int32',
                        'valid_bet': 'float32', 'net_amount': 'float32'})
        if use_date_column:
            df['日期'] = df['日期'].astype('string')

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

        game_types = {
            1: ('体育有效投注', '体育会员输赢'),
            2: ('电竞有效投注', '电竞会员输赢'),
            3: ('真人有效投注', '真人会员输赢'),
            4: ('彩票有效投注', '彩票会员输赢'),
            5: ('棋牌有效投注', '棋牌会员输赢'),
            6: ('电子有效投注', '电子会员输赢'),
            7: ('捕鱼有效投注', '捕鱼会员输赢')
        }

        pivot_index = ['日期', '会员ID'] if use_date_column else ['会员ID']
        valid_pivot = df.pivot_table(index=pivot_index, columns='game_type', values='valid_bet',
                                     aggfunc='sum', fill_value=0, observed=False).reset_index()
        valid_pivot.columns = pivot_index + [game_types.get(col, (str(col),))[0] for col in
                                             valid_pivot.columns[len(pivot_index):]]
        net_pivot = df.pivot_table(index=pivot_index, columns='game_type', values='net_amount',
                                   aggfunc='sum', fill_value=0, observed=False).reset_index()
        net_pivot.columns = pivot_index + [game_types.get(col, (str(col),))[1] for col in
                                           net_pivot.columns[len(pivot_index):]]

        result = member_stats
        for game_type in sorted(game_types.keys()):
            valid_col = game_types[game_type][0]
            net_col = game_types[game_type][1]
            if valid_col in valid_pivot.columns:
                result = result.merge(valid_pivot[pivot_index + [valid_col]], on=pivot_index, how='outer')
            if net_col in net_pivot.columns:
                result = result.merge(net_pivot[pivot_index + [net_col]], on=pivot_index, how='outer')

        final_columns = (['日期', '会员ID', '投注次数', '有效投注', '会员输赢'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢']) + \
                        [col for col in result.columns if col not in (
                            ['日期', '会员ID', '投注次数', '有效投注', '会员输赢'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢'])]
        result = result.reindex(columns=final_columns)

        return result[result['有效投注'] > 0]

    def mongo_betting_details(self) -> pd.DataFrame:
        """查询 MongoDB 投注详细记录，替换游戏详情中的特殊字符，仅保留结算日期"""
        columns = [
            '站点ID', '会员ID', '结算日期', '会员账号', '场馆', '游戏', '赛事ID', '注单号', '赔率',
            '投注额', '有效投注', '会员输赢', '是否提前结算', '投注时间', '开始时间',
            '结算时间', '游戏详情', '游戏完整详情', '联赛', '球队', '玩法'
        ]
        collections = [
            col for col in self.db.list_collection_names()
            if col.startswith(self.mongo_collection_prefix) and col.endswith(self.venue)
        ]
        if not collections:
            return pd.DataFrame(columns=columns)

        pipeline = [
            {"$match": {
                "flag": self.flag_value,
                "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
            }},
            {"$sort": {"bet_time": 1}},
            {"$project": {
                "_id": 0,
                "站点ID": "$site_id",
                "会员ID": "$member_id",
                "结算日期": "$settle_time",
                "会员账号": "$member_name",
                "场馆": "$venue_name",
                "游戏": "$game_name",
                "赛事ID": "$match_id",
                "注单号": "$id",
                "赔率": {"$cond": [{"$eq": ["$odds_type", "EURO"]}, "$odds", {"$add": ["$odds", 1]}]},
                "投注额": "$bet_amount",
                "有效投注": "$valid_bet_amount",
                "会员输赢": "$net_amount",
                "是否提前结算": "$early_settle_flag",
                "投注时间": "$bet_time",
                "开始时间": "$start_time",
                "结算时间": "$settle_time",
                "游戏详情": "$play_info",
                "游戏完整详情": "$game_play_info",
            }}
        ]
        if self.site_id is not None:
            pipeline[0]["$match"]["site_id"] = self.site_id
        df = self._process_mongo_collections(collections, pipeline)
        if df.empty:
            return pd.DataFrame(columns=columns)

        df['游戏详情'] = df['游戏详情'].astype(str).str.replace(' ', ' ')
        df['游戏完整详情'] = df['游戏完整详情'].astype(str).str.replace(' ', ' ')

        def parse_details(row):
            details = str(row['游戏完整详情' if row['场馆'] == 'LHDJ' else '游戏详情']).split('\n')
            if 'TY' in row['场馆']:
                return pd.Series({
                    '联赛': details[1] if len(details) > 0 else '',
                    '球队': details[2] if len(details) > 0 else '',
                    '玩法': details[3] if len(details) > 0 else ''
                })
            elif 'DJ' in row['场馆']:
                return pd.Series({
                    '联赛': details[0] if len(details) > 0 and row['场馆'] == 'LHDJ' else details[1] if len(
                        details) > 0 else '',
                    '球队': details[2] if len(details) > 0 and row['场馆'] == 'LHDJ' else details[2] if len(
                        details) > 0 else '',
                    '玩法': details[4] if len(details) > 0 and row['场馆'] == 'LHDJ' else details[3] if len(
                        details) > 0 else ''
                })
            return pd.Series({'联赛': '', '球队': '', '玩法': ''})

        df = pd.concat([df, df.apply(parse_details, axis=1)], axis=1)
        df['结算日期'] = pd.to_datetime(df['结算日期']).dt.date

        df = df.astype({
            '站点ID': 'string', '会员ID': 'category', '结算日期': 'object', '会员账号': 'string', '场馆': 'string',
            '游戏': 'string', '赛事ID': 'string', '注单号': 'string', '赔率': 'float32',
            '投注额': 'float32', '有效投注': 'float32', '会员输赢': 'float32', '是否提前结算': 'string',
            '投注时间': 'datetime64[ns]', '开始时间': 'datetime64[ns]', '结算时间': 'datetime64[ns]',
            '游戏详情': 'string', '游戏完整详情': 'string', '联赛': 'string',
            '球队': 'string', '玩法': 'string'
        })

        return df[columns]


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
    betting_details = db_query.mongo_betting_details()
    member_info = db_query._1_member_basic_info()

    valid_bet_rules = [
        (328888, 1388),
        (168888, 688),
        (88888, 368),
        (58888, 238),
        (16888, 68),
        (8888, 36),
        (3888, 16),
        (1388, 6)
    ]

    def calculate_valid_bet_bonus(valid_bet):
        for threshold, bonus in valid_bet_rules:
            if valid_bet >= threshold:
                return bonus
        return 0

    daily_valid_bet = betting_details.groupby(['会员ID', '结算日期'])['有效投注'].sum().reset_index()
    daily_valid_bet['彩金奖励'] = daily_valid_bet['有效投注'].apply(calculate_valid_bet_bonus)

    chess_bets = betting_details[
        (betting_details['游戏'].str.contains('棋牌', na=False)) &
        (betting_details['投注额'] >= 100)
    ].copy()

    def calculate_consecutive_wins(group):
        group = group.sort_values('投注时间')
        group['win'] = group['会员输赢'] > 0
        group['win_streak'] = (group['win'] != group['win'].shift(1)).cumsum()
        group['win_count'] = group[group['win']].groupby('win_streak').cumcount() + 1
        group['win_count'] = group['win_count'].fillna(0).astype(int)
        return group

    chess_bets = chess_bets.groupby('会员ID').apply(calculate_consecutive_wins).reset_index(drop=True)

    streak_rules = [
        (16, 0.10),
        (12, 0.08),
        (8, 0.05),
        (5, 0.03),
        (3, 0.02)
    ]

    def calculate_streak_bonus(streak):
        for threshold, ratio in streak_rules:
            if streak >= threshold:
                return ratio
        return 0

    chess_bets['连赢彩金比例'] = chess_bets['win_count'].apply(calculate_streak_bonus)
    streak_bets = chess_bets[chess_bets['连赢彩金比例'] > 0].groupby(['会员ID', '结算日期', '连赢彩金比例'])['投注额'].sum().reset_index()
    streak_bets['连赢彩金'] = streak_bets['投注额'] * streak_bets['连赢彩金比例']
    streak_summary = streak_bets.groupby(['会员ID', '结算日期']).agg({
        '连赢彩金比例': 'max',
        '连赢彩金': 'sum'
    }).reset_index()

    result_df = daily_valid_bet[['会员ID', '结算日期', '彩金奖励']].merge(
        streak_summary[['会员ID', '结算日期', '连赢彩金比例', '连赢彩金']],
        on=['会员ID', '结算日期'],
        how='left'
    ).merge(
        member_info[['会员ID', '会员账号']],
        on='会员ID',
        how='inner'
    )

    result_df['连赢彩金比例'] = result_df['连赢彩金比例'].fillna(0)
    result_df['连赢彩金'] = result_df['连赢彩金'].fillna(0)

    final_columns = ['会员账号', '会员ID', '结算日期', '彩金奖励', '连赢彩金比例', '连赢彩金']
    result_df = result_df[final_columns]

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

