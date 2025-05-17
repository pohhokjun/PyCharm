import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd
import pymongo
from sqlalchemy import create_engine
import pymysql

# 参数配置
site_id = 1000
mongo_uri = "mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/"
MYSQL_URI = 'mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/u1_1000'
db_name = "update_records"

# --------------- MongoDB 按天聚合并处理 ---------------
game_type_map = {
    1: '体育', 2: '电竞', 3: '真人',
    4: '彩票', 5: '棋牌', 6: '电子', 7: '捕鱼'
}

def fetch_coll_daily(coll_name, start_date, end_date):
    client = pymongo.MongoClient(mongo_uri)
    db = client["update_records"]
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
    db = client["update_records"]
    cols = [c for c in db.list_collection_names() if c.startswith("pull_order")]
    client.close()

    # 使用多线程（最大15个线程）并行处理 MongoDB 数据
    with ThreadPoolExecutor(max_workers=15) as ex:
        all_rows = [row for fut in ex.map(lambda coll: fetch_coll_daily(coll, start_date, end_date), cols) for row in fut]

    # 如果没有数据，返回一个空的 DataFrame，但包含所有预期的列
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

    # 如果是前30天数据，则按 member_id 聚合求和
    if period_label == "30days":
        # 计算总有效投注
        total_valid = df.groupby('member_id')['total_valid'].sum().reset_index()
        total_valid.columns = ['member_id', '总有效投注']

        # pivot 七大场馆有效投注
        valid_pivot = df.pivot_table(
            index='member_id',
            columns='game_type',
            values='total_valid',
            aggfunc='sum',
            fill_value=0
        )
        # 确保所有游戏类型的列都存在
        expected_game_types = list(range(1, 8))
        valid_pivot = valid_pivot.reindex(columns=expected_game_types, fill_value=0)
        valid_pivot.columns = [f"{game_type_map.get(c, c)}有效投注" for c in valid_pivot.columns]
        valid_pivot = valid_pivot.reset_index()

        # 合并总有效投注和场馆有效投注
        df_result = pd.merge(total_valid, valid_pivot, on='member_id', how='outer')

        # 新增字段：客户类型
        game_cols = [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
        df_result['客户类型'] = df_result[game_cols].idxmax(axis=1).str.replace('有效投注', '')
        df_result['客户类型'] = df_result.apply(
            lambda row: '无' if row['总有效投注'] == 0 else row['客户类型'], axis=1
        )

        # 新增字段：投注天数
        betting_days = df[df['total_valid'] > 0].groupby('member_id')['date'].nunique().reset_index()
        betting_days.columns = ['member_id', '投注天数']
        df_result = pd.merge(df_result, betting_days, on='member_id', how='left')
        df_result['投注天数'] = df_result['投注天数'].fillna(0).astype(int)

        # 新增字段：均值
        df_result['均值'] = df_result.apply(
            lambda row: row['总有效投注'] / row['投注天数'] if row['投注天数'] > 0 else 0, axis=1
        )

        # 为列名添加前缀，表示前30天数据
        df_result.columns = ['member_id'] + [f"前30天_{col}" if col != 'member_id' else col for col in df_result.columns[1:]]
    else:
        # 昨日数据：计算总有效投注
        total_valid = df.groupby('member_id')['total_valid'].sum().reset_index()
        total_valid.columns = ['member_id', '总有效投注']

        # pivot 七大场馆有效投注
        valid_pivot = df.pivot_table(
            index='member_id',
            columns='game_type',
            values='total_valid',
            aggfunc='sum',
            fill_value=0
        )
        # 确保所有游戏类型的列都存在
        expected_game_types = list(range(1, 8))
        valid_pivot = valid_pivot.reindex(columns=expected_game_types, fill_value=0)
        valid_pivot.columns = [f"{game_type_map.get(c, c)}有效投注" for c in valid_pivot.columns]
        valid_pivot = valid_pivot.reset_index()

        # 合并总有效投注和场馆有效投注
        df_result = pd.merge(total_valid, valid_pivot, on='member_id', how='outer')

        # 新增字段：客户类型
        game_cols = [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
        df_result['客户类型'] = df_result[game_cols].idxmax(axis=1).str.replace('有效投注', '')
        df_result['客户类型'] = df_result.apply(
            lambda row: '无' if row['总有效投注'] == 0 else row['客户类型'], axis=1
        )

        # 为列名添加前缀，表示昨日数据
        df_result.columns = ['member_id'] + [f"昨日_{col}" if col != 'member_id' else col for col in df_result.columns[1:]]

    return df_result

def fetch_member_info():
    engine = create_engine(MYSQL_URI)
    sql = f"""
        SELECT
            id as member_id,
            name as 会员账号,
            vip_grade as VIP等级
        FROM member_info
        WHERE site_id = {site_id};
    """
    """查询会员最后日期"""
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

def fetch_mongo_30days(start_date, end_date):
    return get_mongo_data(start_date, end_date, "30days")

def fetch_mongo_yesterday(start_date, end_date):
    return get_mongo_data(start_date, end_date, "yesterday")

if __name__ == '__main__':
    today = date.today()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # 前30天（不包括昨日）
    last_30_end = (yesterday - timedelta(days=1)).strftime("%Y-%m-%d")
    last_30_start = (yesterday - timedelta(days=30)).strftime("%Y-%m-%d")

    # 使用多进程并行拉取 MongoDB 和 MySQL 数据
    with ProcessPoolExecutor(max_workers=2) as executor:
        # 提交任务并传递参数
        future_30days = executor.submit(fetch_mongo_30days, last_30_start, last_30_end)
        future_yesterday = executor.submit(fetch_mongo_yesterday, yesterday_str, yesterday_str)
        future_mysql = executor.submit(fetch_member_info)

        # 等待所有任务完成
        df_30days = future_30days.result()
        df_yesterday = future_yesterday.result()
        df_member = future_mysql.result()

    # 按照 member_id 合并 MongoDB 和 MySQL 数据
    df_30days = pd.merge(df_member, df_30days, on='member_id', how='right')
    df_yesterday = pd.merge(df_member, df_yesterday, on='member_id', how='right')
    df_final = pd.merge(df_30days, df_yesterday, on=['member_id', '会员账号', 'VIP等级', '最后投注日期'], how='outer')

    # 新增字段：差值（昨日_总有效投注 - 前30天_总有效投注）
    df_final['昨日_差值'] = df_final['昨日_总有效投注'].fillna(0) - df_final['前30天_均值'].fillna(0)

    # 删除 member_id 字段
    df_final = df_final.drop(columns=['member_id'])

    # 调整列顺序
    game_cols = ['总有效投注'] + [f"{game_type_map[i]}有效投注" for i in range(1, 8)]
    final_cols = (
        [f"前30天_{col}" for col in game_cols] +
        ['前30天_客户类型', '前30天_投注天数', '前30天_均值'] +
        ['会员账号', 'VIP等级', '最后投注日期'] +
        [f"昨日_{col}" for col in game_cols] +
        ['昨日_客户类型', '昨日_差值']
    )
    df_final = df_final[final_cols]

    print(df_final)
    # 保存到 Excel 文件
    df_final.to_excel(
        fr".\好博体育\会员数据.xlsx",
        index=False,
        engine="openpyxl",
    )
    print(f"完成：已输出")