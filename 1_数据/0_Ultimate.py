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
                 site_id: int = 1000, start_date: str = '2025-05-01', end_date: str = '2025-05-31',
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
           u1_mi.is_agent AS '是否代理',
           u1_mi.vip_grade AS 'VIP等级',
           (SELECT GROUP_CONCAT(DISTINCT c1_sv.dict_value ORDER BY c1_sv.code SEPARATOR ',')
            FROM control_1000.sys_dict_value c1_sv
            WHERE FIND_IN_SET(c1_sv.code, u1_mi.tag_id)
            AND (c1_sv.initial_flag IS NULL OR c1_sv.initial_flag <> 1)) AS '标签',
           u1_mofr.remark AS '备注',
           CASE WHEN u1_mi.real_name_desensitization <> '' THEN '已绑定' ELSE '' END AS '实名状态',
           CASE WHEN u1_mi.phone_desensitization <> '' THEN '已绑定' ELSE '' END AS '手机状态',
           CASE WHEN u1_mi.email_desensitization <> '' THEN '已绑定' ELSE '' END AS '邮箱状态',
           CASE WHEN u1_mbi.member_info_id IS NOT NULL THEN '已绑定' ELSE '' END AS '银行卡状态',
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
            order_amount AS '首存金额',
            confirm_at AS '首存确认时间'
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
       """
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

    def _3_last_bet_date(self) -> pd.DataFrame:
        """查询会员首存信息"""
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

    def _7_member_stats_history(self) -> pd.DataFrame:
        """查询会员历史累计统计信息"""
        query = f"""
       SELECT
           member_id AS '会员ID',
           COALESCE(SUM(deposit_count), 0) AS '历史存款笔数',
           COALESCE(SUM(deposit), 0) AS '历史存款',
           COALESCE(SUM(draw_count), 0) AS '历史取款笔数',
           COALESCE(SUM(draw), 0) AS '历史取款',
           COALESCE(SUM(bets), 0) AS '历史有效投注金额',
           COALESCE(-SUM(profit), 0) AS '历史会员输赢',
           COALESCE(SUM(promo), 0) AS '历史红利',
           COALESCE(SUM(rebate), 0) AS '历史返水'
       FROM {self.bigdata}.member_daily_statics
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
        # 移除 None 值
        pipeline = [{k: v for k, v in stage.items() if v is not None} for stage in pipeline]

        df = self._process_mongo_collections(collections, pipeline)
        if df.empty:
            return pd.DataFrame(
                columns=['日期', '会员ID', '投注次数', '有效投注', '会员输赢'] if use_date_column else ['会员ID','投注次数','有效投注','会员输赢'])

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

        # 修改排序：按场馆顺序合并有效投注和会员输赢
        result = member_stats
        for game_type in sorted(game_types.keys()):
            valid_col = game_types[game_type][0]
            net_col = game_types[game_type][1]
            if valid_col in valid_pivot.columns:
                result = result.merge(valid_pivot[pivot_index + [valid_col]], on=pivot_index, how='outer')
            if net_col in net_pivot.columns:
                result = result.merge(net_pivot[pivot_index + [net_col]], on=pivot_index, how='outer')

        # 确保最终列顺序
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

        # 替换游戏详情中的特殊字符
        df['游戏详情'] = df['游戏详情'].astype(str).str.replace('&nbsp;', ' ')
        df['游戏完整详情'] = df['游戏完整详情'].astype(str).str.replace('&nbsp;', ' ')

        # 解析联赛、球队、玩法
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
        # ----------------------------------------------------------------------------------------------------
        # # 定义目标联赛
        # target_leagues = [
        #     "英格兰超级联赛", "西班牙甲级联赛", "法国甲级联赛", "德国甲级联赛", "意大利甲级联赛",
        #     "西班牙甲组联赛", "意大利甲组联赛", "法国甲组联赛", "德国甲组联赛"
        # ]
        #
        # # 筛选数据
        # df = df[
        #     # 游戏列包含 "足球"
        #     df['游戏'].str.contains('足球', na=False) &
        #     # 排除游戏详情
        #     ~df['游戏详情'].str.contains('独家|虚拟|VR', na=False) &
        #     # 联赛在目标联赛列表中
        #     df['联赛'].str.contains('|'.join(re.escape(l) for l in target_leagues), na=False, regex=True)
        #     ]
        # ----------------------------------------------------------------------------------------------------
        # 仅保留结算日期（无时间）
        df['结算日期'] = pd.to_datetime(df['结算日期']).dt.date

        # 类型优化
        df = df.astype({
            '站点ID': 'string', '会员ID': 'category', '结算日期': 'object', '会员账号': 'string', '场馆': 'string',
            '游戏': 'string', '赛事ID': 'string', '注单号': 'string', '赔率': 'float32',
            '投注额': 'float32', '有效投注': 'float32', '会员输赢': 'float32', '是否提前结算': 'string',
            '投注时间': 'datetime64[ns]', '开始时间': 'datetime64[ns]', '结算时间': 'datetime64[ns]',
            '游戏详情': 'string', '游戏完整详情': 'string', '联赛': 'string',
            '球队': 'string', '玩法': 'string'
        })

        # 确保字段顺序
        return df[columns]

    def _11_member_dividend(self) -> pd.DataFrame:
        """查询会员历史累计统计信息，筛选指定 site_id"""
        query = f"""
        SELECT
            a1_md.site_id AS 站点ID,
            a1_md.activity_title AS 活动标题,
            a1_oci.title AS 标题,
            a1_md.member_id AS 会员ID,
            a1_md.member_name AS 会员账号,
            a1_md.member_grade AS 会员等级,
            a1_md.bill_no AS 订单号,
            CASE
                WHEN a1_md.wallet_category = 1 THEN '中心钱包'
                WHEN a1_md.wallet_category = 2 THEN '场馆钱包'
                ELSE a1_md.wallet_category
            END AS 钱包类别,
            a1_md.flow_times AS 流水倍数,
            a1_md.money AS 红利金额,
            CASE
                WHEN a1_md.status = 1 THEN '处理中'
                WHEN a1_md.status = 2 THEN '成功'
                WHEN a1_md.status = 3 THEN '失败'
                ELSE a1_md.status
            END AS 状态,
            CASE
                WHEN a1_md.issue_type = 1 THEN '手动发放'
                WHEN a1_md.issue_type = 2 THEN '自动发放'
                ELSE a1_md.issue_type
            END AS 发行类型,
            COALESCE(sv.dict_value, a1_md.activity_type) AS '活动类型',
            a1_md.created_at AS 申请时间,
            a1_md.applicant_remark AS 申请备注,
            a1_md.updated_at AS 发放时间,
            a1_md.check_user AS 审核用户,
            a1_md.check_remark AS 审核备注,
            a1_md.applicant AS 操作人
        FROM activity_1000.member_dividend a1_md
        LEFT JOIN activity_1000.operation_activity_info a1_oci
            ON a1_md.activity_id = a1_oci.id
        LEFT JOIN (
            SELECT code, dict_value
            FROM (
                SELECT
                    code,
                    dict_value,
                    dict_code,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY code) AS rn
                FROM control_1000.sys_dict_value
                WHERE dict_code IN ('activity_type')
            ) t
            WHERE rn = 1
        ) sv ON a1_md.activity_type = sv.code
        WHERE a1_md.updated_at BETWEEN '{self.start_time}' AND '{self.end_time}'
        AND a1_md.category NOT IN (999555)
        """
        if self.site_id is not None:
            query += f" AND a1_md.site_id = {self.site_id}"
        return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)


def save_to_excel(df: pd.DataFrame, filename: str):
    """保存 DataFrame 到 Excel 文件"""
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        df.drop(columns=['member_id'], errors='ignore').to_excel(writer, sheet_name='Sheet1', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        worksheet.freeze_panes(1, 0)
        worksheet.autofilter(0, 0, 0, len(df.columns) - 1)


def send_to_telegram(file_path: str, bot_token: str, chat_id: str) -> bool:
    """发送 Excel 文件到 Telegram 群"""
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    try:
        with open(file_path, 'rb') as file:
            files = {'document': file}
            data = {'chat_id': chat_id, 'caption': f'数据输出: {os.path.basename(file_path)}'}
            response = requests.post(url, data=data, files=files)
            if response.status_code == 200:
                print(f"Excel file sent to Telegram successfully: {file_path}")
                return True
            else:
                print(f"Failed to send Excel to Telegram: {response.text}")
                return False
    except Exception as e:
        print(f"Error sending Excel to Telegram: {e}")
        return False


def work(db_query: DatabaseQuery) -> pd.DataFrame:
    """执行查询并合并结果"""
    #推广架构 db_query._0_promotion()
    #会员信息 .merge(db_query._1_member_basic_info(), on='会员ID', how='inner')
    #首存 .merge(db_query._2_first_deposit(), on='会员ID', how='left')
    #最后投注 .merge(db_query._3_last_bet_date(), on='会员ID', how='left')
    #会员数据（时间段） .merge(db_query._6_member_stats_period(use_date_column=True False), on='会员ID', how='inner')
    #会员数据（历史） .merge(db_query._7_member_stats_history(), on='会员ID', how='inner')
    #游戏场馆 .merge(db_query.mongo_betting_stats(use_date_column=True False), on=['会员ID', '日期'], how='inner')
    #注单 .merge(db_query.mongo_betting_details(), on=['会员ID', '日期'], how='inner')
    #红利 .merge(db_query._11_member_dividend(), on='会员ID', how='inner')
    result_df = (db_query._1_member_basic_info()
                 .merge(db_query._2_first_deposit(), on='会员ID', how='left')
                 .merge(db_query._3_last_bet_date(), on='会员ID', how='left')
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
    TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
    CHAT_ID = '7523061850'
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
