import os
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pandas as pd
import pymongo
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

# 参数配置
site_id = 1000
db_uri = "mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/"
db_name = "update_records"

def basic_data(start_date: str, end_date: str) -> pd.DataFrame:
    """拉取平台日报（MySQL），并按日期聚合。"""
    engine = create_engine(
        "mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/bigdata"
    )

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
    print(df)

    # 只保留需要的聚合列
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

    # 只重命名那 19 个指标列
    group_data = group_data.rename(columns={
        '投注人数(结算)': '投注人数',
        '投注额(结算)': '投注额',
        '有效投注(结算)': '有效投注',
        '公司输赢含提前结算(结算)': '公司输赢',
        '提前结算(结算)': '提前结算',
        # 其余聚合列名称保持一致或随你需要改
    })

    # 最后再挑选我们想要输出的列
    group_data = group_data[[
        '日期', '首存人数', '存款人数', '存款额', '投注人数', '有效投注'
    ]]
    return group_data

def bet_count(start_date: str, end_date: str) -> pd.DataFrame:
    engine = create_engine(
        "mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/bigdata"
    )
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
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
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
                    "篮球",  # 标识篮球组
                    "体育"   # 其余归为体育组
                ]}
            }},
            {"$group": {
                "_id": {"date": "$date", "group": "$group"},
                "valid_bet": {"$sum": "$valid_bet_amount"},
                "unique_members": {"$addToSet": "$member_id"}  # 收集唯一 member_id
            }},
            {"$addFields": {
                "bets": {"$size": "$unique_members"}  # 计算唯一 member_id 数量
            }},
            {"$project": {"_id": 0, "date": "$_id.date", "group": "$_id.group", "valid_bet": 1, "bets": 1}},
            {"$sort": {"date": 1}}
        ]
        recs.extend(db[coll].aggregate(pipe))
    client.close()

    df = pd.DataFrame(recs)
    df['日期'] = pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d")

    # 透视
    pivot = df.pivot_table(
        index='日期',
        columns='group',
        values=['bets', 'valid_bet'],
        aggfunc='sum',
        fill_value=0
    )

    # 重命名列：投注人数在前，有效投注在后，去掉下划线
    pivot.columns = [
        f"{grp}投注人数" if metric == 'bets' else f"{grp}有效投注"
        for metric, grp in pivot.columns
    ]
    # 按固定顺序排序： 体育投注人数、体育有效投注、篮球投注人数、篮球有效投注
    ordered_cols = ['体育投注人数', '体育有效投注', '篮球投注人数', '篮球有效投注']
    result = pivot.reset_index()[['日期'] + ordered_cols]
    return result

def other_data(start_date: str, end_date: str) -> pd.DataFrame:
    """拉取其他五组（game_type 2-6），并按日期聚合。投注人数在前，有效投注在后，去除下划线。"""
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
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
                "member_id": "$member_id"  # 确保 member_id 字段可用
            }},
            {"$group": {
                "_id": {"date": "$date", "gt": "$gt"},
                "total_valid": {"$sum": "$vb"},
                "unique_members": {"$addToSet": "$member_id"}  # 收集唯一 member_id
            }},
            {"$addFields": {
                "daily_active": {"$size": "$unique_members"}  # 计算唯一 member_id 数量
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
    df['日期'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

    # 按类型汇总
    agg = (
        df.groupby(['日期', 'gt'])
        .agg({'daily_active': 'sum', 'total_valid': 'sum'})
        .reset_index()
    )

    # 透视
    pivot = agg.pivot(
        index='日期',
        columns='gt',
        values=['daily_active', 'total_valid']
    )

    # 游戏类型映射与重命名，投注人数在前，有效投注在后
    map_name = {2: '电竞', 3: '真人', 4: '彩票', 5: '棋牌', 6: '电子'}
    pivot.columns = [
        f"{map_name[gt]}投注人数" if metric == 'daily_active'
        else f"{map_name[gt]}有效投注"
        for metric, gt in pivot.columns
    ]

    # 按固定顺序排列：电竞、真人、彩票、棋牌、电子
    ordered = []
    for name in ['电竞', '真人', '彩票', '棋牌', '电子']:
        ordered += [f"{name}投注人数", f"{name}有效投注"]

    result = pivot.reset_index()[['日期'] + ordered]
    return result

def merge_period(start_date: str, end_date: str) -> pd.DataFrame:
    """拉取三张表并按日期合并"""
    # 并行拉三表
    with ProcessPoolExecutor(max_workers=3) as exe:
        f1 = exe.submit(basic_data, start_date, end_date)
        f2 = exe.submit(bet_count, start_date, end_date)
        f3 = exe.submit(sports_data, start_date, end_date)
        f4 = exe.submit(other_data, start_date, end_date)
        df1 = f1.result();
        df2 = f2.result();
        df3 = f3.result();
        df4 = f4.result()

    merged = (df1.merge(df2, on='日期', how='outer')
              .merge(df3, on='日期', how='outer')
              .merge(df4, on='日期', how='outer')
              .sort_values('日期'))
    return merged


if __name__ == '__main__':
    today = date.today()
    yesterday = today - timedelta(days=1)

    # **统一按“昨天”来算，本月 = 昨天所在月的 1 日～昨天；上月 = 昨天所在月的上一整月**
    this_start = yesterday.replace(day=1).strftime("%Y-%m-%d")
    this_end = yesterday.strftime("%Y-%m-%d")

    last_month_end = (yesterday.replace(day=1) - timedelta(days=1))
    last_month_start = last_month_end.replace(day=1)
    last_start = last_month_start.strftime("%Y-%m-%d")
    last_end = last_month_end.strftime("%Y-%m-%d")

    # 拉取本月和上月
    df_this = merge_period(this_start, this_end)
    df_last = merge_period(last_start, last_end)

    # 合并数据
    final = pd.concat([df_last, df_this], ignore_index=True)
    # 确保'日期'字段是 datetime 类型
    final['日期'] = pd.to_datetime(final['日期'])
    # 添加'星期'字段，自动识别日期对应的星期几（1-7）
    final['星期'] = final['日期'].dt.dayofweek + 1  # 星期一=1, ..., 星期日=7
    # 添加辅助列：月份和日
    final['月份'] = final['日期'].dt.month
    final['日'] = final['日期'].dt.day
    # 按月份降序、日升序排序
    final = final.sort_values(by=['月份', '日'], ascending=[False, True])
    # 删除辅助列
    final = final.drop(columns=['月份', '日'])
    # 格式化'日期'为 "YYYY-MM-DD"
    final['日期'] = final['日期'].dt.strftime('%Y-%m-%d')
    # 将'星期'字段移动到'日期'字段后面
    date_col_idx = final.columns.get_loc('日期')
    final.insert(date_col_idx + 1, '星期', final.pop('星期'))

    # 直接重命名字段为指定的中文名称
    final.columns = [
        '日期', '星期', '首存人数', '存款人数', '存款金额', '投注人数', '有效投注', '投注笔数', '体育日活',
        '体育有效', '篮球日活', '篮球有效', '电竞日活', '电竞有效', '真人日活', '真人有效',
        '彩票日活', '彩票有效', '棋牌日活', '棋牌有效', '电子日活', '电子有效'
    ]
    # 调整字段顺序
    desired_order = [
        '日期', '星期', '首存人数', '存款人数', '存款金额', '投注人数', '有效投注', '投注笔数',
        '体育日活', '体育有效', '篮球日活', '篮球有效', '电竞日活', '电竞有效', '电子日活', '电子有效',
        '彩票日活', '彩票有效', '棋牌日活', '棋牌有效', '真人日活', '真人有效'
    ]
    final = final[desired_order]

    # 设置输出文件路径并保存
    final.to_excel(
        fr".\好博体育\平台数据\平台数据.xlsx",
        index=False,
        engine="openpyxl",
    )
    print(f"完成：已输出")
