import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
import pymongo
from pathlib import Path

# ------------------- 配置 -------------------
site_id = 1000
MYSQL_URI = 'mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/bigdata'
MONGO_URI = 'mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/'
DB_NAME = 'update_records'
OUTPUT_PATH = '.\好博体育'
TABLE_NAME = 'platform_daily_report'
MYSQL_URI_U1 = 'mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/u1_1000'

# 确保输出目录存在
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ------------------- 日期计算 -------------------
today = date.today()
yesterday = today - timedelta(days=1)
first_day_current_month = yesterday.replace(day=1)
first_day_previous_month = first_day_current_month - relativedelta(months=1)
last_day_previous_month = first_day_current_month - timedelta(days=1)

first_day_current_month_start = first horno_current_month.strftime('%Y-%m-%d')
yesterday_end = yesterday.strftime('%Y-%m-%d')
first_day_previous_month_start = first_day_previous_month.strftime('%Y-%m-%d')
last_day_previous_month_end = last_day_previous_month.strftime('%Y-%m-%d')

# 前30天（不包括昨日）
last_30_end = (yesterday - timedelta(days=1)).strftime('%Y-%m-%d')
last_30_start = (yesterday - timedelta(days=30)).strftime('%Y-%m-%d')

# ------------------- 部门数据函数 -------------------
def member_type():
    """拉取 MySQL 平台日报，做简单映射翻译后返回 DataFrame"""
    engine = create_engine(MYSQL_URI)
    sql = f"""
        SELECT
            statics_date,
            top_code,
            first_recharge_member_count,
            recharge_member_count,
            recharge_amount,
            bet_member_count_settle,
            valid_bet_amount_settle
        FROM {TABLE_NAME}
        WHERE site_id = {site_id}
          AND (
                statics_date BETWEEN '{first_day_current_month_start}' AND '{yesterday_end}'
             OR statics_date BETWEEN '{first_day_previous_month_start}' AND '{last_day_previous_month_end}'
          )
        ORDER BY statics_date
    """
    df = pd.read_sql(sql, engine)
    df = df.rename(columns={
        'statics_date': '日期',
        'top_code': '部门',
        'first_recharge_member_count': '首存人数',
        'recharge_member_count': '存款人数',
        'recharge_amount': '存款金额',
        'bet_member_count_settle': '投注人数',
        'valid_bet_amount_settle': '有效投注'
    })
    df['部门'] = df['部门'].map({-1: '直客', 0: '普代', 1: '官代'})
    engine.dispose()
    return df

def department_data():
    """拉取 MySQL agent_department + top_daily_report，按照规则拆分、聚合后返回 DataFrame"""
    engine = create_engine(MYSQL_URI)
    sql = f"""
    SELECT
        a1_ad.group_name AS '1级',
        a1_ad_2.group_name AS '部门',
        tdr.statics_date AS '日期',
        SUM(tdr.first_recharge_member_count) AS '首存人数',
        SUM(tdr.recharge_member_count) AS '存款人数',
        SUM(tdr.recharge_amount) AS '存款金额',
        SUM(tdr.bet_member_count_settle) AS '投注人数',
        SUM(tdr.valid_bet_amount_settle) AS '有效投注'
    FROM agent_1000.agent_department a1_ad
    LEFT JOIN agent_1000.agent_department a1_ad_2
        ON a1_ad_2.pid = a1_ad.id
    LEFT JOIN agent_1000.agent_department a1_ad_3
        ON a1_ad_3.pid = a1_ad_2.id
    LEFT JOIN agent_1000.agent_department a1_ad_4
        ON a1_ad_4.pid = a1_ad_3.id
    LEFT JOIN agent_1000.agent_dept_member a1_adm
        ON a1_adm.dept_id = COALESCE(a1_ad_4.id, a1_ad_3.id, a1_ad_2.id, a1_ad.id)
    LEFT JOIN bigdata.top_daily_report tdr
        ON tdr.agent_name = a1_adm.agent_name
    WHERE
        a1_ad.level = 1
        AND a1_ad.group_name IN ('官资部', '推广部', '招商部')
        AND tdr.statics_date BETWEEN 
            DATE_SUB(DATE_FORMAT(CURDATE(), '%%Y-%%m-01'), INTERVAL 1 MONTH)
            AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        AND tdr.site_id = {site_id}
    GROUP BY
        a1_ad.group_name,
        a1_ad_2.group_name,
        a1_ad_3.group_name,
        tdr.statics_date
    ORDER BY
        tdr.statics_date
    """
    df = pd.read_sql(sql, engine)

    numeric_cols = ['首存人数', '存款人数', '存款金额', '投注人数', '有效投注']

    # 1️⃣ “官资部” 全部日期聚合
    df_office = (
        df[df['1级'] == '官资部']
        .groupby('日期', as_index=False)[numeric_cols].sum()
    )
    df_office['部门'] = '官资部'

    # 2️⃣ “推广部” 保留指定子部门
    promote_keep = {'推广1部', '推广3部', '推广5部', '推广6部', '推广7部', '推广9部', '推广11部', '推广12部'}
    df_promote = df[
        (df['1级'] == '推广部') & df['部门'].isin(promote_keep)
    ][['日期', '部门'] + numeric_cols].copy()

    # 3️⃣ “招商部” 全部日期聚合
    df_invest = (
        df[df['1级'] == '招商部']
        .groupby('日期', as_index=False)[numeric_cols].sum()
    )
    df_invest['部门'] = '招商部'

    # 4️⃣ 合并三部分
    df_result = pd.concat([df_office, df_promote, df_invest], ignore_index=True)
    return df_result[['部门', '日期'] + numeric_cols]

def process_department_data():
    """处理部门数据并生成Excel"""
    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(member_type)
        f2 = executor.submit(department_data)
        df_member = f1.result()
        df_dept = f2.result()

    df_merged = pd.concat([df_member, df_dept], ignore_index=True)
    cols = df_merged.columns.tolist()
    cols.remove('部门')
    cols.remove('日期')
    df_merged = df_merged[['部门', '日期'] + cols]

    numeric_cols = [c for c in df_merged.columns if c not in ['部门', '日期']]
    final = (
        df_merged
        .groupby(['部门', '日期'], as_index=False)[numeric_cols]
        .sum()
    )

    final['日期'] = pd.to_datetime(final['日期'])
    final['星期'] = final['日期'].dt.dayofweek + 1
    idx = final.columns.get_loc('有效投注') + 1
    final.insert(idx, '星期', final.pop('星期'))

    final['月'] = final['日期'].dt.month
    dept_special = {'直客': 1000, '普代': 1001, '官代': 1002}
    final['部门排序'] = final['部门'].apply(lambda x: dept_special.get(x, 0))

    final = final.sort_values(
        by=['月', '部门排序'],
        ascending=[False, True]
    )

    final['日期'] = final['日期'].dt.strftime('%Y-%m-%d')
    final = final.drop(columns=['月', '部门排序'])

    final.to_excel(
        os.path.join(OUTPUT_PATH, '部门数据.xlsx'),
        index=False,
        engine="openpyxl",
    )
    return "部门数据完成"

# ------------------- 会员数据函数 -------------------
game_type_map = {
    1: '体育', 2: '电竞', 3: '真人',
    4: '彩票', 5: '棋牌', 6: '电子', 7: '捕鱼'
}

def fetch_coll_daily(coll_name, start_date, end_date):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    pipeline = [
        {"$match": {
            "flag": 1,
            "site_id": site_id,
            "settle_time": {
                "$gte": f"{start_date} 00:00:00",
                "$lte": f"{end_date} 23:59:59"
            }
        }},
        {"$project": {
            "date": {"$substr": ["$settle_time", 0, 10]},
            "member_id": 1,
            "game_type": 1,
            "valid_bet_amount": 1
        }},
        {"$group": {
            "_id": {
                "member_id": "$member_id",
                "date": "$date",
                "game_type": "$game_type"
            },
            "total_valid": {"$sum": "$valid_bet_amount"}
        }}
    ]
    rows = list(db[coll_name].aggregate(pipeline))
    client.close()
    return rows

def get_mongo_data(start_date, end_date, period_label):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    cols = [c for c in db.list_collection_names() if c.startswith("pull_order")]
    client.close()

    with ThreadPoolExecutor(max_workers=15) as ex:
        all_rows = [row for fut in ex.map(lambda coll: fetch_coll_daily(coll, start_date, end_date), cols) for row in fut]

    if not all_rows:
        game_cols = ['总有效投注'] + [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
        prefix = "前30天_" if period_label == "30days" else "昨日_"
        columns = ['member_id'] + [f"{prefix}{col}" for col in game_cols]
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame([{
        "member_id": r["_id"]["member_id"],
        "date": r["_id"]["date"],
        "game_type": r["_id"]["game_type"],
        "total_valid": r["total_valid"]
    } for r in all_rows])

    if period_label == "30days":
        total_valid = df.groupby('member_id')['total_valid'].sum().reset_index()
        total_valid.columns = ['member_id', '总有效投注']

        valid_pivot = df.pivot_table(
            index='member_id',
            columns='game_type',
            values='total_valid',
            aggfunc='sum',
            fill_value=0
        )
        expected_game_types = list(range(1, 8))
        valid_pivot = valid_pivot.reindex(columns=expected_game_types, fill_value=0)
        valid_pivot.columns = [f"{game_type_map.get(c, c)}有效投注" for c in valid_pivot.columns]
        valid_pivot = valid_pivot.reset_index()

        df_result = pd.merge(total_valid, valid_pivot, on='member_id', how='outer')

        game_cols = [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
        df_result['客户类型'] = df_result[game_cols].idxmax(axis=1).str.replace('有效投注', '')
        df_result['客户类型'] = df_result.apply(
            lambda row: '无' if row['总有效投注'] == 0 else row['客户类型'], axis=1
        )

        betting_days = df[df['total_valid'] > 0].groupby('member_id')['date'].nunique().reset_index()
        betting_days.columns = ['member_id', '投注天数']
        df_result = pd.merge(df_result, betting_days, on='member_id', how='left')
        df_result['投注天数'] = df_result['投注天数'].fillna(0).astype(int)

        df_result['均值'] = df_result.apply(
            lambda row: row['总有效投注'] / row['投注天数'] if row['投注天数'] > 0 else 0, axis=1
        )

        df_result.columns = ['member_id'] + [f"前30天_{col}" if col != 'member_id' else col for col in df_result.columns[1:]]
    else:
        total_valid = df.groupby('member_id')['total_valid'].sum().reset_index()
        total_valid.columns = ['member_id', '总有效投注']

        valid_pivot = df.pivot_table(
            index='member_id',
            columns='game_type',
            values='total_valid',
            aggfunc='sum',
            fill_value=0
        )
        expected_game_types = list(range(1, 8))
        valid_pivot = valid_pivot.reindex(columns=expected_game_types, fill_value=0)
        valid_pivot.columns = [f"{game_type_map.get(c, c)}有效投注" for c in valid_pivot.columns]
        valid_pivot = valid_pivot.reset_index()

        df_result = pd.merge(total_valid, valid_pivot, on='member_id', how='outer')

        game_cols = [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
        df_result['客户类型'] = df_result[game_cols].idxmax(axis=1).str.replace('有效投注', '')
        df_result['客户类型'] = df_result.apply(
            lambda row: '无' if row['总有效投注'] == 0 else row['客户类型'], axis=1
        )

        df_result.columns = ['member_id'] + [f"昨日_{col}" if col != 'member_id' else col for col in df_result.columns[1:]]

    return df_result

def fetch_member_info():
    engine = create_engine(MYSQL_URI_U1)
    sql = f"""
        SELECT
            id as member_id,
            name as 会员账号,
            vip_grade as VIP等级
        FROM member_info
        WHERE site_id = {site_id};
    """
    query = f"""
           SELECT 
            member_id,
            MAX(CASE WHEN valid_bet_amount > 0 THEN statics_date ELSE NULL END) AS '最后投注日期'
           FROM bigdata.member_daily_statics
           WHERE site_id = {site_id}
           GROUP BY member_id;
          """
    df_info = pd.read_sql(sql, engine)
    df_last_date = pd.read_sql(query, engine)
    df = pd.merge(df_info, df_last_date, on='member_id', how='outer')
    return df

def process_member_data():
    """处理会员数据并生成Excel"""
    with ProcessPoolExecutor(max_workers=3) as executor:
        future_30days = executor.submit(get_mongo_data, last_30_start, last_30_end, "30days")
        future_yesterday = executor.submit(get_mongo_data, yesterday_end, yesterday_end, "yesterday")
        future_mysql = executor.submit(fetch_member_info)
        df_30days = future_30days.result()
        df_yesterday = future_yesterday.result()
        df_member = future_mysql.result()

    df_30days = pd.merge(df_member, df_30days, on='member_id', how='right')
    df_yesterday = pd.merge(df_member, df_yesterday, on='member_id', how='right')
    df_final = pd.merge(df_30days, df_yesterday, on=['member_id', '会员账号', 'VIP等级', '最后投注日期'], how='outer')

    df_final['昨日_差值'] = df_final['昨日_总有效投注'].fillna(0) - df_final['前30天_均值'].fillna(0)
    df_final = df_final.drop(columns=['member_id'])

    game_cols = ['总有效投注'] + [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
    final_cols = (
        [f"前30天_{col}" for col in game_cols] +
        ['前30天_客户类型', '前30天_投注天数', '前30天_均值'] +
        ['会员账号', 'VIP等级', '最后投注日期'] +
        [f"昨日_{col}" for col in game_cols] +
        ['昨日_客户类型', '昨日_差值']
    )
    df_final = df_final[final_cols]

    df_final.to_excel(
        os.path.join(OUTPUT_PATH, '会员数据.xlsx'),
        index=False,
        engine="openpyxl",
    )
    return "会员数据完成"

# ------------------- 平台数据函数 -------------------
def basic_data(start_date: str, end_date: str) -> pd.DataFrame:
    """拉取平台日报（MySQL），并按日期聚合。"""
    engine = create_engine(MYSQL_URI)
    query = f"""
    SELECT *
      FROM bigdata.platform_daily_report
     WHERE site_id = {site_id}
       AND statics_date BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql_query(query, engine)
    df.columns = [
        'id', '日期', '站点ID', '渠道类型',
        '存款调整上分金额', '存款调整下分金额',
        '存款额', '取款额', '注册数',
        '首存人数', '首存额',
        '首次有效充值会员数', '首次有效充值金额',
        '存款人数', '取款人数', '投注人数',
        '有效投注', '投注额', '公司输赢含提前结算', '红利',
        '返水', '个人佣金金额', '团队佣金金额',
        '提前结算净额', '账户调整',
        '首次充值注册比例', '人均首次充值',
        '存提差', '提款充值比例',
        '净投注金额比例', '公司输赢', '集团分成',
        '团队金额比例', '场馆费', '投注人数(结算)',
        '有效投注(结算)', '投注额(结算)', '公司输赢含提前结算(结算)',
        '提前结算(结算)', '公司输赢(结算)',
        '盈余比例(结算)', '场馆费(结算)', '打赏收入',
        '集团分成(结算)', '存款手续费', '提款手续费', '分成比例'
    ]

    group_data = df.groupby(['站点ID', '日期']).agg({
        '注册数': 'sum',
        '首存人数': 'sum',
        '首存额': 'sum',
        '存款人数': 'sum',
        '存款额': 'sum',
        '取款人数': 'sum',
        '取款额': 'sum',
        '存提差': 'sum',
        '账户调整': 'sum',
        '投注人数(结算)': 'sum',
        '投注额(结算)': 'sum',
        '有效投注(结算)': 'sum',
        '公司输赢含提前结算(结算)': 'sum',
        '提前结算(结算)': 'sum',
        '红利': 'sum',
        '返水': 'sum',
        '个人佣金金额': 'sum',
        '团队佣金金额': 'sum',
        '打赏收入': 'sum'
    }).reset_index()

    group_data = group_data.rename(columns={
        '投注人数(结算)': '投注人数',
        '投注额(结算)': '投注额',
        '有效投注(结算)': '有效投注',
        '公司输赢含提前结算(结算)': '公司输赢',
        '提前结算(结算)': '提前结算',
    })

    group_data = group_data[[
        '日期', '首存人数', '存款人数', '存款额', '投注人数', '有效投注'
    ]]
    return group_data

def bet_count(start_date: str, end_date: str) -> pd.DataFrame:
    engine = create_engine(MYSQL_URI)
    query = f"""
       SELECT statics_date as 日期,
       SUM(bet_count_settle) as 投注次数
        FROM bigdata.game_record_report
        WHERE site_id = {site_id}
          AND statics_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY statics_date
       """
    df = pd.read_sql_query(query, engine)
    return df

def sports_data(start_date: str, end_date: str) -> pd.DataFrame:
    """体育组 & 篮球组（MongoDB）按日期聚合并拆成两列，投注人数在前，有效投注在后。"""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    st, et = f"{start_date} 00:00:00", f"{end_date} 23:59:59"
    cols = [c for c in db.list_collection_names() if c.startswith('pull_order') and c.endswith('TY')]

    recs = []
    for coll in cols:
        pipe = [
            {"$match": {"flag": 1, "site_id": site_id, "settle_time": {"$gte": st, "$lte": et}}},
            {"$addFields": {
                "date": {"$substr": ["$settle_time", 0, 10]},
                "group": {"$cond": [
                    {"$in": ["$game_name", ["篮球", "籃球/美足"]]},
                    "篮球",
                    "体育"
                ]}
            }},
            {"$group": {
                "_id": {"date": "$date", "group": "$group"},
                "valid_bet": {"$sum": "$valid_bet_amount"},
                "unique_members": {"$addToSet": "$member_id"}
            }},
            {"$addFields": {
                "bets": {"$size": "$unique_members"}
            }},
            {"$project": {"_id": 0, "date": "$_id.date", "group": "$_id.group", "valid_bet": 1, "bets": 1}},
            {"$sort": {"date": 1}}
        ]
        recs.extend(db[coll].aggregate(pipe))
    client.close()

    df = pd.DataFrame(recs)
    df['日期'] = pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d")

    pivot = df.pivot_table(
        index='日期',
        columns='group',
        values=['bets', 'valid_bet'],
        aggfunc='sum',
        fill_value=0
    )

    pivot.columns = [
        f"{grp}投注人数" if metric == 'bets' else f"{grp}有效投注"
        for metric, grp in pivot.columns
    ]
    ordered_cols = ['体育投注人数', '体育有效投注', '篮球投注人数', '篮球有效投注']
    result = pivot.reset_index()[['日期'] + ordered_cols]
    return result

def other_data(start_date: str, end_date: str) -> pd.DataFrame:
    """拉取其他五组（game_type 2-6），并按日期聚合。投注人数在前，有效投注在后。"""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    st, et = f"{start_date} 00:00:00", f"{end_date} 23:59:59"
    cols = [c for c in db.list_collection_names() if c.startswith("pull_order")]

    def agg_one(coll):
        pipe = [
            {"$match": {
                "flag": 1, "site_id": site_id,
                "settle_time": {"$gte": st, "$lte": et},
                "game_type": {"$in": [2, 3, 4, 5, 6]}
            }},
            {"$project": {
                "date": {"$substr": ["$settle_time", 0, 10]},
                "gt": "$game_type",
                "vb": "$valid_bet_amount",
                "member_id": "$member_id"
            }},
            {"$group": {
                "_id": {"date": "$date", "gt": "$gt"},
                "total_valid": {"$sum": "$vb"},
                "unique_members": {"$addToSet": "$member_id"}
            }},
            {"$addFields": {
                "daily_active": {"$size": "$unique_members"}
            }},
            {"$project": {
                "_id": 0, "date": "$_id.date", "gt": "_id.gt",
                "total_valid": 1, "daily_active": 1
            }}
        ]
        return list(db[coll].aggregate(pipe))

    recs = []
    with ThreadPoolExecutor(max_workers=10) as exe:
        for docs in exe.map(agg_one, cols):
            recs.extend(docs)
    client.close()

    df = pd.DataFrame(recs)
    df['日期'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

    agg = (
        df.groupby(['日期', 'gt'])
        .agg({'daily_active': 'sum', 'total_valid': 'sum'})
        .reset_index()
    )

    pivot = agg.pivot(
        index='日期',
        columns='gt',
        values=['daily_active', 'total_valid']
    )

    map_name = {2: '电竞', 3: '真人', 4: '彩票', 5: '棋牌', 6: '电子'}
    pivot.columns = [
        f"{map_name[gt]}投注人数" if metric == 'daily_active'
        else f"{map_name[gt]}有效投注"
        for metric, gt in pivot.columns
    ]

    ordered = []
    for name in ['电竞', '真人', '彩票', '棋牌', '电子']:
        ordered += [f"{name}投注人数", f"{name}有效投注"]

    result = pivot.reset_index()[['日期'] + ordered]
    return result

def merge_period(start_date: str, end_date: str):
    """拉取三张表并按日期合并"""
    with ProcessPoolExecutor(max_workers=4) as exe:
        f1 = exe.submit(basic_data, start_date, end_date)
        f2 = exe.submit(bet_count, start_date, end_date)
        f3 = exe.submit(sports_data, start_date, end_date)
        f4 = exe.submit(other_data, start_date, end_date)
        df1 = f1.result()
        df2 = f2.result()
        df3 = f3.result()
        df4 = f4.result()

    merged = (df1.merge(df2, on='日期', how='outer')
              .merge(df3, on='日期', how='outer')
              .merge(df4, on='日期', how='outer')
              .sort_values('日期'))
    return merged

def process_platform_data():
    """处理平台数据并生成Excel"""
    this_start = first_day_current_month_start
    this_end = yesterday_end
    last_month_end = last_day_previous_month
    last_month_start = first_day_previous_month
    last_start = last_month_start.strftime("%Y-%m-%d")
    last_end = last_month_end.strftime("%Y-%m-%d")

    df_this = merge_period(this_start, this_end)
    df_last = merge_period(last_start, last_end)

    final = pd.concat([df_last, df_this], ignore_index=True)
    final['日期'] = pd.to_datetime(final['日期'])
    final['星期'] = final['日期'].dt.dayofweek + 1
    final['月份'] = final['日期'].dt.month
    final['日'] = final['日期'].dt.day

    final = final.sort_values(by=['月份', '日'], ascending=[False, True])
    final = final.drop(columns=['月份', '日'])
    final['日期'] = final['日期'].dt.strftime('%Y-%m-%d')

    date_col_idx = final.columns.get_loc('日期')
    final.insert(date_col_idx + 1, '星期', final.pop('星期'))

    final.columns = [
        '日期', '星期', '首存人数', '存款人数', '存款金额', '投注人数', '有效投注', '投注笔数', '体育日活',
        '体育有效', '篮球日活', '篮球有效', '电竞日活', '电竞有效', '真人日活', '真人有效',
        '彩票日活', '彩票有效', '棋牌日活', '棋牌有效', '电子日活', '电子有效'
    ]

    desired_order = [
        '日期', '星期', '首存人数', '存款人数', '存款金额', '投注人数', '有效投注', '投注笔数',
        '体育日活', '体育有效', '篮球日活', '篮球有效', '电竞日活', '电竞有效', '电子日活', '电子有效',
        '彩票日活', '彩票有效', '棋牌日活', '棋牌有效', '真人日活', '真人有效'
    ]
    final = final[desired_order]

    final.to_excel(
        os.path.join(OUTPUT_PATH, '平台数据.xlsx'),
        index=False,
        engine="openpyxl",
    )
    return "平台数据完成"

# ------------------- 主程序 -------------------
if __name__ == '__main__':
    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(process_department_data),
            executor.submit(process_member_data),
            executor.submit(process_platform_data)
        ]
        for future in futures:
            print(future.result())