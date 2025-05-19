
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd
import pymongo
from sqlalchemy import create_engine
import pymysql
from dateutil.relativedelta import relativedelta

# 参数配置
site_id = 1000
mongo_uri = "mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/"
MYSQL_URI_U1_1000 = 'mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/u1_1000'
MYSQL_URI_BIGDATA = 'mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/bigdata'
db_name_mongo = "update_records"

# --------------- MongoDB 按天聚合并处理 (会员数据) ---------------
game_type_map = {
    1: '体育', 2: '电竞', 3: '真人',
    4: '彩票', 5: '棋牌', 6: '电子', 7: '捕鱼'
}

def fetch_coll_daily(coll_name, start_date, end_date):
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name_mongo]
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
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name_mongo]
    cols = [c for c in db.list_collection_names() if c.startswith("pull_order")]
    client.close()

    with ThreadPoolExecutor(max_workers=15) as ex:
        all_rows = [row for fut in ex.map(lambda coll: fetch_coll_daily(coll, start_date, end_date), cols) for row in fut]

    if not all_rows:
        # Original logic for empty DataFrame
        game_cols = ['总有效投注'] + [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
        prefix = "前30天_" if period_label == "30days" else "昨日_"
        columns = ['member_id'] + [f"{prefix}{col}" for col in game_cols]
        # Add specific columns based on period_label as in original
        if period_label == "30days":
             columns += [f"{prefix}客户类型", f"{prefix}投注天数", f"{prefix}均值"]
        else: # yesterday
             columns += [f"{prefix}客户类型"]
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
    else: # yesterday
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
    engine = create_engine(MYSQL_URI_U1_1000)
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
    engine.dispose()
    return df

def fetch_mongo_30days(start_date, end_date):
    return get_mongo_data(start_date, end_date, "30days")

def fetch_mongo_yesterday(start_date, end_date):
    return get_mongo_data(start_date, end_date, "yesterday")

# --------------- 部门数据处理 ---------------
def member_type():
    engine = create_engine(MYSQL_URI_BIGDATA)
    sql = f"""
        SELECT
            statics_date,
            top_code,
            first_recharge_member_count,
            recharge_member_count,
            recharge_amount,
            bet_member_count_settle,
            valid_bet_amount_settle
        FROM platform_daily_report
        WHERE site_id = {site_id}
          AND (
                statics_date BETWEEN DATE_FORMAT(CURDATE(), '%%Y-%%m-01') AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
             OR statics_date BETWEEN DATE_SUB(DATE_FORMAT(CURDATE(), '%%Y-%%m-01'), INTERVAL 1 MONTH) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
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
    engine = create_engine(MYSQL_URI_BIGDATA)
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
    FROM agent_{site_id}.agent_department a1_ad
    LEFT JOIN agent_{site_id}.agent_department a1_ad_2
        ON a1_ad_2.pid = a1_ad.id
    LEFT JOIN agent_{site_id}.agent_department a1_ad_3
        ON a1_ad_3.pid = a1_ad_2.id
    LEFT JOIN agent_{site_id}.agent_department a1_ad_4
        ON a1_ad_4.pid = a1_ad_3.id
    LEFT JOIN agent_{site_id}.agent_dept_member a1_adm
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

    numeric_cols = ['首存人数','存款人数','存款金额','投注人数','有效投注']

    df_office = (
        df[df['1级']=='官资部']
        .groupby('日期', as_index=False)[numeric_cols].sum()
    )
    df_office['部门'] = '官资部'

    promote_keep = {'推广1部','推广3部','推广5部','推广6部','推广7部','推广9部','推广11部','推广12部'}
    df_promote = df[
        (df['1级']=='推广部') & df['部门'].isin(promote_keep)
    ][['日期','部门']+numeric_cols].copy()

    df_invest = (
        df[df['1级'] == '招商部']
        .groupby('日期', as_index=False)[numeric_cols].sum()
    )
    df_invest['部门'] = '招商部'

    df_result = pd.concat([df_office, df_promote, df_invest], ignore_index=True)
    engine.dispose()
    return df_result[['部门','日期']+numeric_cols]

# --------------- 平台数据处理 ---------------
def basic_data(start_date: str, end_date: str) -> pd.DataFrame:
    engine = create_engine(MYSQL_URI_BIGDATA)

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
    engine.dispose()
    return group_data

def bet_count(start_date: str, end_date: str) -> pd.DataFrame:
    engine = create_engine(MYSQL_URI_BIGDATA)
    query = f"""
       SELECT statics_date as 日期,
       SUM(bet_count_settle) as 投注次数
        FROM bigdata.game_record_report
        WHERE site_id = {site_id}
          AND statics_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY statics_date
       """
    df = pd.read_sql_query(query, engine)
    engine.dispose()
    return df

def sports_data(start_date: str, end_date: str) -> pd.DataFrame:
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name_mongo]
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
        recs.extend(list(db[coll].aggregate(pipe)))

    client.close()

    df = pd.DataFrame(recs)
    if df.empty:
        return pd.DataFrame(columns=['日期', '体育投注人数', '体育有效投注', '篮球投注人数', '篮球有效投注'])

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
    # Ensure all ordered columns exist, add if missing with 0
    for col in ordered_cols:
        if col not in pivot.columns:
            pivot[col] = 0

    result = pivot.reset_index()[['日期'] + ordered_cols]
    return result

def other_data(start_date: str, end_date: str) -> pd.DataFrame:
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name_mongo]
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
                "_id": 0, "date": "$_id.date", "gt": "$_id.gt",
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
    if df.empty:
        map_name = {2: '电竞', 3: '真人', 4: '彩票', 5: '棋牌', 6: '电子'}
        ordered = []
        for name in ['电竞', '真人', '彩票', '棋牌', '电子']:
            ordered += [f"{name}投注人数", f"{name}有效投注"]
        return pd.DataFrame(columns=['日期'] + ordered)

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

    # Ensure all ordered columns exist, add if missing with 0
    for col in ordered:
        if col not in pivot.columns:
            pivot[col] = 0

    result = pivot.reset_index()[['日期'] + ordered]
    return result

def merge_period(start_date: str, end_date: str) -> pd.DataFrame:
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

# --------------- 主执行逻辑 ---------------
if __name__ == '__main__':
    # 确保输出目录存在
    output_dir_member = Path(r".\好博体育\会员数据")
    output_dir_member.mkdir(parents=True, exist_ok=True)
    output_dir_dept = Path(r".\好博体育\部门数据")
    output_dir_dept.mkdir(parents=True, exist_ok=True)
    output_dir_platform = Path(r".\好博体育\平台数据")
    output_dir_platform.mkdir(parents=True, exist_ok=True)

    # --- 会员数据部分 ---
    today = date.today()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    last_30_end = (yesterday - timedelta(days=1)).strftime("%Y-%m-%d")
    last_30_start = (yesterday - timedelta(days=30)).strftime("%Y-%m-%d")

    with ProcessPoolExecutor(max_workers=2) as executor:
        future_30days = executor.submit(fetch_mongo_30days, last_30_start, last_30_end)
        future_yesterday = executor.submit(fetch_mongo_yesterday, yesterday_str, yesterday_str)
        future_mysql = executor.submit(fetch_member_info)

        df_30days = future_30days.result()
        df_yesterday = future_yesterday.result()
        df_member = future_mysql.result()

    # Ensure member_id is int type for merging
    if not df_member.empty:
        df_member['member_id'] = df_member['member_id'].astype(int)
    if not df_30days.empty:
        df_30days['member_id'] = df_30days['member_id'].astype(int)
    if not df_yesterday.empty:
        df_yesterday['member_id'] = df_yesterday['member_id'].astype(int)

    # --- Replicate original merge logic for Member Data ---
    # Original script merges df_member with df_30days (how='right')
    df_30days_merged = pd.merge(df_member, df_30days, on='member_id', how='right')

    # Original script merges df_member with df_yesterday (how='right')
    df_yesterday_merged = pd.merge(df_member, df_yesterday, on='member_id', how='right')

    # Original script merges the two results (how='outer') on multiple keys
    df_final_member = pd.merge(df_30days_merged, df_yesterday_merged, on=['member_id', '会员账号', 'VIP等级', '最后投注日期'], how='outer')
    # --- End of replicated merge logic ---

    # Fill NaNs for numeric columns before calculations
    numeric_cols_member = [col for col in df_final_member.columns if '有效投注' in col or '均值' in col or '差值' in col or '投注天数' in col]
    df_final_member[numeric_cols_member] = df_final_member[numeric_cols_member].fillna(0)

    # Calculate '昨日_差值'
    df_final_member['昨日_差值'] = df_final_member['昨日_总有效投注'] - df_final_member['前30天_均值']

    # Ensure all game type columns exist before calculating 客户类型 and fill NaNs with 0
    game_cols_base = [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
    game_cols_30days_prefixed = [f"前30天_{col}" for col in game_cols_base]
    game_cols_yesterday_prefixed = [f"昨日_{col}" for col in game_cols_base]

    all_game_cols_prefixed = game_cols_30days_prefixed + game_cols_yesterday_prefixed + ['前30天_总有效投注', '昨日_总有效投注']
    for col in all_game_cols_prefixed:
        if col not in df_final_member.columns:
            df_final_member[col] = 0 # Add missing game type columns with 0

    # Recalculate 客户类型 after ensuring columns exist and filling NaNs
    # Apply idxmax only if there are game columns to check
    if game_cols_30days_prefixed and not df_final_member[game_cols_30days_prefixed].empty:
        df_final_member['前30天_客户类型'] = df_final_member[game_cols_30days_prefixed].idxmax(axis=1).str.replace('前30天_', '').str.replace('有效投注', '')
    else:
        df_final_member['前30天_客户类型'] = '无' # Default if no game columns or empty

    if game_cols_yesterday_prefixed and not df_final_member[game_cols_yesterday_prefixed].empty:
         df_final_member['昨日_客户类型'] = df_final_member[game_cols_yesterday_prefixed].idxmax(axis=1).str.replace('昨日_', '').str.replace('有效投注', '')
    else:
         df_final_member['昨日_客户类型'] = '无' # Default if no game columns or empty


    # Apply '无' if total valid is 0
    df_final_member['前30天_客户类型'] = df_final_member.apply(
        lambda row: '无' if row['前30天_总有效投注'] == 0 else row['前30天_客户类型'], axis=1
    )
    df_final_member['昨日_客户类型'] = df_final_member.apply(
        lambda row: '无' if row['昨日_总有效投注'] == 0 else row['昨日_客户类型'], axis=1
    )


    df_final_member = df_final_member.drop(columns=['member_id'])

    # Define final column order based on original script
    game_cols_base_ordered = ['总有效投注'] + [f"{game_type_map[i]}有效投注" for i in [1, 2, 3, 4, 5, 6, 7]] # Ensure game types 1-7 are in order

    final_cols_member_order = (
        [f"前30天_{col}" for col in game_cols_base_ordered] +
        ['前30天_客户类型', '前30天_投注天数', '前30天_均值'] +
        ['会员账号', 'VIP等级', '最后投注日期'] +
        [f"昨日_{col}" for col in game_cols_base_ordered] +
        ['昨日_客户类型', '昨日_差值']
    )
    # Ensure all columns in the desired order exist, add if missing (should be handled by previous steps, but defensive)
    for col in final_cols_member_order:
        if col not in df_final_member.columns:
            df_final_member[col] = None # Use None for object types like strings, 0 for numeric

    df_final_member = df_final_member[final_cols_member_order]

    df_final_member.to_excel(
        output_dir_member / "会员数据.xlsx",
        index=False,
        engine="openpyxl",
    )
    print(f"完成：会员数据已输出")

    # --- 部门数据部分 ---
    # Date calculations are internal to the SQL queries in this section

    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(member_type)
        f2 = executor.submit(department_data)
        df_member_dept = f1.result()
        df_dept_data = f2.result()

    df_merged_dept = pd.concat([df_member_dept, df_dept_data], ignore_index=True)

    cols_dept = df_merged_dept.columns.tolist()
    # Handle cases where '部门' or '日期' might be missing if no data (unlikely with current SQL)
    if '部门' in cols_dept: cols_dept.remove('部门')
    if '日期' in cols_dept: cols_dept.remove('日期')

    # Ensure '部门' and '日期' are the first columns if they exist
    ordered_cols_dept_initial = []
    if '部门' in df_merged_dept.columns: ordered_cols_dept_initial.append('部门')
    if '日期' in df_merged_dept.columns: ordered_cols_dept_initial.append('日期')
    ordered_cols_dept_initial.extend(cols_dept)
    df_merged_dept = df_merged_dept[ordered_cols_dept_initial]


    numeric_cols_dept = [c for c in df_merged_dept.columns if c not in ['部门', '日期']]
    final_dept = (
        df_merged_dept
        .groupby(['部门', '日期'], as_index=False)[numeric_cols_dept]
        .sum()
    )

    final_dept['日期'] = pd.to_datetime(final_dept['日期'])

    final_dept['星期'] = final_dept['日期'].dt.dayofweek + 1
    # Find insertion index safely based on '有效投注'
    idx_dept = final_dept.columns.get_loc('有效投注') + 1 if '有效投注' in final_dept.columns else len(final_dept.columns)
    # Ensure '星期' column exists before trying to pop it
    if '星期' in final_dept.columns:
        final_dept.insert(idx_dept, '星期', final_dept.pop('星期'))


    final_dept['月'] = final_dept['日期'].dt.month
    dept_special = {'直客': 1000, '普代': 1001, '官代': 1002}
    final_dept['部门排序'] = final_dept['部门'].apply(lambda x: dept_special.get(x, 0))

    # --- Revert sorting to match original script exactly ---
    # Original script sorted by ['月', '部门排序']
    final_dept = final_dept.sort_values(
        by=['月', '部门排序'],
        ascending=[False, True]
    )
    # --- End of sorting revert ---


    final_dept['日期'] = final_dept['日期'].dt.strftime('%Y-%m-%d')

    final_dept = final_dept.drop(columns=['月', '部门排序'])

    final_dept.to_excel(
        output_dir_dept / "部门数据.xlsx",
        index=False,
        engine="openpyxl",
    )
    print(f"完成：部门数据已输出")

    # --- 平台数据部分 ---
    today_platform = date.today()
    yesterday_platform = today_platform - timedelta(days=1)

    this_start_platform = yesterday_platform.replace(day=1).strftime("%Y-%m-%d")
    this_end_platform = yesterday_platform.strftime("%Y-%m-%d")

    last_month_end_platform = (yesterday_platform.replace(day=1) - timedelta(days=1))
    last_month_start_platform = last_month_end_platform.replace(day=1)
    last_start_platform = last_month_start_platform.strftime("%Y-%m-%d")
    last_end_platform = last_month_end_platform.strftime("%Y-%m-%d")

    df_this_platform = merge_period(this_start_platform, this_end_platform)
    df_last_platform = merge_period(last_start_platform, last_end_platform)

    final_platform = pd.concat([df_last_platform, df_this_platform], ignore_index=True)

    final_platform['日期'] = pd.to_datetime(final_platform['日期'])
    final_platform['星期'] = final_platform['日期'].dt.dayofweek + 1
    final_platform['月份'] = final_platform['日期'].dt.month
    final_platform['日'] = final_platform['日期'].dt.day

    final_platform = final_platform.sort_values(by=['月份', '日'], ascending=[False, True])

    final_platform = final_platform.drop(columns=['月份', '日'])
    final_platform['日期'] = final_platform['日期'].dt.strftime('%Y-%m-%d')

    # Find insertion index safely for '星期' after '日期'
    date_col_idx_platform = final_platform.columns.get_loc('日期') if '日期' in final_platform.columns else -1
    if '星期' in final_platform.columns and date_col_idx_platform != -1:
         final_platform.insert(date_col_idx_platform + 1, '星期', final_platform.pop('星期'))


    # Ensure all expected columns exist before renaming and reordering
    # Reconstruct expected columns based on merge_period output and individual data functions
    # Basic data columns
    expected_cols_platform_before_rename = ['日期', '首存人数', '存款人数', '存款金额', '投注人数', '有效投注']
    # Bet count column
    expected_cols_platform_before_rename.append('投注次数')
    # Sports data columns
    sports_cols = ['体育投注人数', '体育有效投注', '篮球投注人数', '篮球有效投注']
    expected_cols_platform_before_rename.extend(sports_cols)
    # Other data columns
    other_game_types = ['电竞', '真人', '彩票', '棋牌', '电子']
    other_cols = [f"{name}投注人数" for name in other_game_types] + [f"{name}有效投注" for name in other_game_types]
    expected_cols_platform_before_rename.extend(other_cols)


    # Add any columns present in the DataFrame but not in our expected list (just in case)
    # This helps retain unexpected columns if they somehow appeared in the original
    current_cols_platform = final_platform.columns.tolist()
    for col in current_cols_platform:
        if col not in expected_cols_platform_before_rename and col not in ['星期', '月份', '日']: # Exclude temp columns
             expected_cols_platform_before_rename.append(col)


    # Ensure all columns from the expected list exist in the DataFrame, add if missing with 0
    for col in expected_cols_platform_before_rename:
         if col not in final_platform.columns:
             final_platform[col] = 0 # Add missing columns with default 0


    # Now safely rename columns
    # Create a dictionary mapping current names to desired names
    rename_map = {
        '投注次数': '投注笔数',
        '体育投注人数': '体育日活',
        '体育有效投注': '体育有效',
        '篮球投注人数': '篮球日活',
        '篮球有效投注': '篮球有效',
        '电竞投注人数': '电竞日活',
        '电竞有效投注': '电竞有效',
        '真人投注人数': '真人日活',
        '真人有效投注': '真人有效',
        '彩票投注人数': '彩票日活',
        '彩票有效投注': '彩票有效',
        '棋牌投注人数': '棋牌日活',
        '棋牌有效投注': '棋牌有效',
        '电子投注人数': '电子日活',
        '电子有效投注': '电子有效',
    }
    # Apply rename only for columns that exist
    final_platform = final_platform.rename(columns={k: v for k, v in rename_map.items() if k in final_platform.columns})


    # Adjust field order based on the desired order
    desired_order_platform = [
        '日期', '星期', '首存人数', '存款人数', '存款金额', '投注人数', '有效投注', '投注笔数',
        '体育日活', '体育有效', '篮球日活', '篮球有效', '电竞日活', '电竞有效', '电子日活', '电子有效',
        '彩票日活', '彩票有效', '棋牌日活', '棋牌有效', '真人日活', '真人有效'
    ]
    # Ensure all desired columns exist before selecting
    for col in desired_order_platform:
         if col not in final_platform.columns:
             final_platform[col] = 0 # Add missing columns with default 0

    final_platform = final_platform[desired_order_platform]


    final_platform.to_excel(
        output_dir_platform / "平台数据.xlsx",
        index=False,
        engine="openpyxl",
    )
    print(f"完成：平台数据已输出")

