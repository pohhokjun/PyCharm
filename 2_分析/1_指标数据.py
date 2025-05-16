
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pymongo
from concurrent.futures import ProcessPoolExecutor
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- Global Configuration ----------
MYSQL_URI = "mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/bigdata"
MONGO_URI = "mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/"
MONGO_DB = "update_records"
SITE_ID = 1000
FLAG = 1
SITE_MAP = {
    1000: '好博体育', 2000: '黄金体育', 3000: '宾利体育', 4000: 'HOME体育',
    5000: '亚洲之星', 6000: '玖博体育', 7000: '蓝火体育', 8000: 'A7体育'
}
CHUNK_SIZE = 10000  # For chunked SQL reading

# Time configurations
today = datetime.now()
yesterday = today - timedelta(days=1)
start_date = yesterday - timedelta(days=29)
start_time = start_date.strftime('%Y-%m-%d 00:00:00')
end_time = yesterday.strftime('%Y-%m-%d 23:59:59')

# Initialize database connections
mysql_engine = create_engine(MYSQL_URI)
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]

# ---------- Daily Report Processing ----------
def fetch_and_process_daily_report():
    logger.info("Processing daily report...")
    query = f"SELECT * FROM bigdata.platform_daily_report WHERE site_id = {SITE_ID};"
    columns = [
        'id', '日期', '站点ID', '渠道类型', '存款调整上分金额', '存款调整下分金额', '存款额', '取款额', '注册数',
        '首存人数', '首存额', '首次有效充值会员数', '首次有效充值金额', '存款人数', '取款人数', '投注人数',
        '有效投注', '投注额', '公司输赢含提前结算', '红利', '返水', '个人佣金金额', '团队佣金金额', '提前结算净额',
        '账户调整', '首次充值注册比例', '人均首次充值', '存提差', '提款充值比例', '净投注金额比例', '公司输赢',
        '集团分成', '团队金额比例', '场馆费', '投注人数(结算)', '有效投注(结算)', '投注额(结算)',
        '公司输赢含提前结算(结算)', '提前结算(结算)', '公司输赢(结算)', '盈余比例(结算)', '场馆费(结算)',
        '打赏收入', '集团分成(结算)', '存款手续费', '提款手续费', '分成比例'
    ]
    group_data = pd.DataFrame()
    for chunk in pd.read_sql_query(query, mysql_engine, chunksize=CHUNK_SIZE):
        chunk.columns = columns
        grouped = chunk.groupby(['站点ID', '日期']).agg({
            '注册数': 'sum', '首存人数': 'sum', '首存额': 'sum', '存款人数': 'sum', '存款额': 'sum',
            '取款人数': 'sum', '取款额': 'sum', '存提差': 'sum', '账户调整': 'sum', '投注人数(结算)': 'sum',
            '投注额(结算)': 'sum', '有效投注(结算)': 'sum', '公司输赢含提前结算(结算)': 'sum',
            '提前结算(结算)': 'sum', '红利': 'sum', '返水': 'sum', '个人佣金金额': 'sum',
            '团队佣金金额': 'sum', '打赏收入': 'sum'
        }).reset_index()
        group_data = pd.concat([group_data, grouped], ignore_index=True)

    # Aggregate again to ensure correctness
    group_data = group_data.groupby(['站点ID', '日期']).sum().reset_index()
    group_data['转化率'] = np.where(
        group_data['注册数'] != 0,
        (group_data['首存人数'] / group_data['注册数']).map('{:.2%}'.format),
        '0.00%'
    )
    group_data['人均首存'] = np.where(
        group_data['首存人数'] != 0,
        (group_data['首存额'] / group_data['首存人数']).round(2),
        0
    )
    group_data['提存率'] = np.where(
        group_data['存款额'] != 0,
        (group_data['取款额'] / group_data['存款额']).map('{:.2%}'.format),
        '0.00%'
    )
    group_data['盈余比例'] = np.where(
        group_data['投注额(结算)'] != 0,
        (group_data['公司输赢含提前结算(结算)'] / group_data['投注额(结算)']).map('{:.2%}'.format),
        '0.00%'
    )
    group_data['公司输赢'] = group_data['公司输赢含提前结算(结算)'] - group_data['提前结算(结算)']
    group_data['集团分成比例'] = '12%'
    group_data['集团分成(结算)'] = (group_data['公司输赢含提前结算(结算)'] + group_data['账户调整']) * 0.12
    group_data['代理佣金'] = group_data['个人佣金金额'] + group_data['团队佣金金额']
    group_data['公司净收入'] = (
        group_data['公司输赢含提前结算(结算)'] + group_data['账户调整'] -
        group_data['红利'] - group_data['返水'] - group_data['代理佣金'] - group_data['集团分成(结算)']
    )
    group_data = group_data.drop(columns=['个人佣金金额', '团队佣金金额'])
    group_data.columns = [
        '站点ID', '日期', '注册数', '首存人数', '首存额', '存款人数', '存款额', '取款人数', '取款额',
        '存提差', '账户调整', '投注人数', '投注额', '有效投注额', '公司输赢含提前结算', '提前结算',
        '红利', '返水', '打赏收入', '转化率', '人均首存', '提存率', '盈余比例', '公司输赢',
        '集团分成比例', '集团分成', '代理佣金', '公司净收入'
    ]
    group_data = group_data[[
        '站点ID', '日期', '注册数', '首存人数', '转化率', '首存额', '人均首存', '存款人数',
        '取款人数', '存款额', '取款额', '存提差', '提存率', '投注人数', '投注额', '有效投注额',
        '公司输赢含提前结算', '盈余比例', '账户调整', '红利', '返水', '代理佣金', '打赏收入',
        '集团分成比例', '集团分成', '公司净收入'
    ]]
    group_data['日期'] = pd.to_datetime(group_data['日期'])
    return group_data

# ---------- Report Generation Functions ----------
def report_basic_data(group_data, yesterday):
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    before_yester = (yesterday - timedelta(days=1)).strftime("%Y-%m-%d")
    last_month = (yesterday - relativedelta(months=1)).strftime("%Y-%m-%d")
    dates = pd.to_datetime([yesterday_str, before_yester, last_month])
    mask = group_data['日期'].isin(dates)
    basic = group_data.loc[mask, [
        '站点ID', '日期', '注册数', '首存人数', '投注人数', '投注额', '有效投注额',
        '公司输赢含提前结算', '公司净收入'
    ]].copy()
    basic['日期'] = basic['日期'].dt.strftime("%Y-%m-%d")
    return basic

def report_revenue_metrics(group_data, yesterday):
    group_data['日期'] = pd.to_datetime(group_data['日期'])
    current_start = yesterday - timedelta(days=7)
    current_end = yesterday
    last_month_start = current_start - relativedelta(months=1)
    last_month_end = current_end - relativedelta(months=1)
    mask_current = (group_data['日期'] >= current_start) & (group_data['日期'] <= current_end)
    mask_last = (group_data['日期'] >= last_month_start) & (group_data['日期'] <= last_month_end)
    filtered_data = pd.concat([group_data[mask_current], group_data[mask_last]])
    filtered_data['日期'] = filtered_data['日期'].dt.strftime("%Y-%m-%d")
    revenue = filtered_data[['站点ID', '日期', '公司输赢含提前结算', '公司净收入']]
    operational = filtered_data[['站点ID', '日期', '存款额', '存款人数']]
    bet = filtered_data[['站点ID', '日期', '有效投注额']]
    event = filtered_data[['站点ID', '日期', '红利', '返水']]
    return revenue, operational, bet, event

def conversion_rate(group_data, yesterday):
    group_data['日期'] = pd.to_datetime(group_data['日期'])
    current_start = yesterday - timedelta(days=7)
    current_end = yesterday
    current_period = group_data[
        (group_data['日期'] >= current_start) & (group_data['日期'] <= current_end)
    ].copy()
    current_period['转化率'] = (
        current_period['转化率'].str.rstrip('%').astype(float).round(0).astype(int).astype(str) + '%'
    )
    current_period['日期'] = current_period['日期'].dt.strftime("%Y-%m-%d")
    return current_period[['站点ID', '日期', '注册数', '首存人数', '转化率']]

def site_specific_basic_data(basic_data):
    site_dfs = {}
    for sid, site_name in SITE_MAP.items():
        sub_df = basic_data[basic_data['站点ID'] == sid]
        if not sub_df.empty:
            site_dfs[f"{site_name}基本数据"] = sub_df
    return site_dfs

# ---------- Retention Calculation ----------
def calc_period_retention(first_dep, daily_df, offset_start, offset_end):
    dd_grouped = daily_df.groupby('date')['member_id'].apply(set).to_dict()
    retention = {}
    for deposit_date, grp in first_dep.groupby('date'):
        new_members = set(grp['member_id'])
        window_ids = set()
        base = datetime.strptime(deposit_date, "%Y-%m-%d")
        for d in range(offset_start, offset_end + 1):
            key = (base + timedelta(days=d)).strftime("%Y-%m-%d")
            window_ids |= dd_grouped.get(key, set())
        kept = len(new_members & window_ids) if new_members else 0
        rate = int((kept / len(new_members)) * 100) if new_members else 0
        retention[deposit_date] = f"{rate}%"
    return retention

def build_retention_table(args):
    first_dep, daily_df = args
    r1 = calc_period_retention(first_dep, daily_df, 1, 7)
    r2 = calc_period_retention(first_dep, daily_df, 8, 14)
    r3 = calc_period_retention(first_dep, daily_df, 15, 21)
    return pd.DataFrame({
        '首存日期': list(r1.keys()),
        '第一周留存': list(r1.values()),
        '第二周留存': list(r2.values()),
        '第三周留存': list(r3.values()),
    })

def fetch_retention_data():
    query1 = f"""
    SELECT member_id, first_deposit_time AS 首存时间
    FROM (
        SELECT member_id, first_deposit_time,
               ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY statics_date DESC) AS rn
        FROM member_daily_statics
        WHERE site_id = {SITE_ID}
          AND first_deposit_time BETWEEN '{start_time}' AND '{end_time}'
    ) AS t WHERE rn = 1;
    """
    query2 = f"""
    SELECT member_id, DATE(confirm_at) AS date, SUM(pay_amount) AS 存款额
    FROM finance_1000.finance_pay_records
    WHERE pay_status IN (2, 3)
      AND confirm_at BETWEEN '{start_time}' AND '{end_time}'
      AND site_id = {SITE_ID}
    GROUP BY member_id, DATE(confirm_at);
    """
    first_deposit = pd.read_sql_query(query1, mysql_engine)
    daily_deposit = pd.read_sql_query(query2, mysql_engine)
    first_deposit['首存时间'] = pd.to_datetime(first_deposit['首存时间'])
    first_deposit['date'] = first_deposit['首存时间'].dt.strftime('%Y-%m-%d')
    daily_deposit['date'] = pd.to_datetime(daily_deposit['date']).dt.strftime('%Y-%m-%d')

    cols = [col for col in mongo_db.list_collection_names() if col.startswith('pull_order')]
    rows = []
    for coll in cols:
        pipeline = [
            {"$match": {"flag": FLAG, "site_id": SITE_ID, "settle_time": {"$gte": start_time, "$lte": end_time}}},
            {"$project": {"settle_time": 1, "member_id": 1, "valid_bet_amount": 1}},
            {"$group": {"_id": {"date": {"$substr": ["$settle_time", 0, 10]}, "member_id": "$member_id"},
                        "total_valid_bet_amount": {"$sum": "$valid_bet_amount"}}},
            {"$sort": {"_id.date": 1}}
        ]
        for doc in mongo_db[coll].aggregate(pipeline):
            rows.append({
                "date": doc["_id"]["date"],
                "member_id": doc["_id"]["member_id"],
                "有效投注": doc["total_valid_bet_amount"]
            })
    member_daily_bet = pd.DataFrame(rows).groupby(['date', 'member_id'])['有效投注'].sum().reset_index()
    member_daily_bet = member_daily_bet[member_daily_bet['有效投注'] > 0]
    return first_deposit, daily_deposit, member_daily_bet

# ---------- Active Days Calculation ----------
def compute_periods(reference):
    today = reference
    yesterday = today - timedelta(days=1)
    this_month_start = today.replace(day=1)
    if today.day == 1:
        this_start_date = this_month_start - relativedelta(months=1)
        this_end_date = this_month_start - timedelta(days=1)
        last_start_date = this_month_start - relativedelta(months=2)
        last_end_date = this_start_date - timedelta(days=1)
    else:
        this_start_date = this_month_start
        this_end_date = yesterday
        last_start_date = this_month_start - relativedelta(months=1)
        last_end_date = last_start_date + relativedelta(months=1) - timedelta(days=1)
    fmt = "%Y-%m-%d"
    return (
        ("本月", (f"{this_start_date.strftime(fmt)} 00:00:00", f"{this_end_date.strftime(fmt)} 23:59:59")),
        ("上月", (f"{last_start_date.strftime(fmt)} 00:00:00", f"{last_end_date.strftime(fmt)} 23:59:59"))
    )

def load_member_daily_bet(start_time, end_time):
    cols = [c for c in mongo_db.list_collection_names() if c.startswith('pull_order')]
    rows = []
    for coll in cols:
        pipeline = [
            {"$match": {"flag": FLAG, "site_id": SITE_ID, "settle_time": {"$gte": start_time, "$lte": end_time}}},
            {"$project": {"settle_time": 1, "member_id": 1, "valid_bet_amount": 1}},
            {"$group": {"_id": {"date": {"$substr": ["$settle_time", 0, 10]}, "member_id": "$member_id"},
                        "total_valid_bet_amount": {"$sum": "$valid_bet_amount"}}},
            {"$sort": {"_id.date": 1}}
        ]
        for doc in mongo_db[coll].aggregate(pipeline):
            rows.append({
                "date": doc["_id"]["date"],
                "member_id": doc["_id"]["member_id"],
                "有效投注": doc["total_valid_bet_amount"]
            })
    df = pd.DataFrame(rows).groupby(['date', 'member_id'])['有效投注'].sum().reset_index()
    return df[df['有效投注'] > 0]

def load_first_deposit(cutoff_date):
    query = f"""
    SELECT member_id, first_deposit_time AS 首存时间
    FROM (
        SELECT member_id, first_deposit_time,
               ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY statics_date DESC) AS rn
        FROM member_daily_statics
        WHERE site_id = {SITE_ID}
          AND first_deposit_time < '{cutoff_date} 00:00:00'
          AND first_deposit_time IS NOT NULL
    ) AS t WHERE rn = 1;
    """
    df = pd.read_sql_query(query, mysql_engine)
    df = df.dropna(subset=['首存时间'])
    df = df[df['首存时间'].astype(str).str.strip() != ""]
    return df

def compute_active_days(member_daily_bet, first_deposit):
    counts = member_daily_bet.groupby('member_id').size()
    common = counts.index.intersection(first_deposit['member_id'])
    dist = counts.loc[common].value_counts().sort_index()
    return pd.DataFrame({"活跃天数": dist.index, "会员数": dist.values})

def task_for_period(args):
    label, period, cutoff = args
    logger.info(f"Processing {label}: {period[0]} to {period[1]}")
    daily_bet = load_member_daily_bet(period[0], period[1])
    first_dep = load_first_deposit(cutoff)
    active = compute_active_days(daily_bet, first_dep)
    return label, active

# ---------- Main Workflow ----------
def main():
    logger.info("Starting data processing...")
    # Process daily report
    group_data = fetch_and_process_daily_report()
    basic_data = report_basic_data(group_data, yesterday)
    revenue_metrics, operational_metrics, bet_metrics, event_metrics = report_revenue_metrics(group_data, yesterday)
    user_metrics = conversion_rate(group_data, yesterday)
    site_dfs = site_specific_basic_data(basic_data)

    # Process retention data
    first_deposit, daily_deposit, member_daily_bet = fetch_retention_data()
    with ProcessPoolExecutor(max_workers=2) as executor:
        retention_results = executor.map(build_retention_table, [
            (first_deposit, daily_deposit),
            (first_deposit, member_daily_bet)
        ])
    df_dep, df_bet = retention_results

    # Process active days
    periods = compute_periods(date.today())
    period_tasks = [(label, period, period[0][:10]) for label, period in periods]
    with ProcessPoolExecutor(max_workers=2) as executor:
        active_results = list(executor.map(task_for_period, period_tasks))
    (label1, df1), (label2, df2) = active_results
    df1.columns = [f"{label1}{col}" for col in df1.columns]
    df2.columns = [f"{label2}{col}" for col in df2.columns]
    active_days_df = pd.concat([df2, df1], axis=1)

    # Write all data to a single Excel file
    with pd.ExcelWriter("1_指标数据.xlsx", engine="xlsxwriter") as writer:
        basic_data.to_excel(writer, sheet_name="基本数据", index=False)
        revenue_metrics.to_excel(writer, sheet_name="营收指标", index=False)
        operational_metrics.to_excel(writer, sheet_name="运营指标", index=False)
        bet_metrics.to_excel(writer, sheet_name="投注指标", index=False)
        user_metrics.to_excel(writer, sheet_name="用户指标", index=False)
        event_metrics.to_excel(writer, sheet_name="活动指标", index=False)
        df_dep.to_excel(writer, sheet_name="存款留存周指标", index=False)
        df_bet.to_excel(writer, sheet_name="投注留存周指标", index=False)
        active_days_df.to_excel(writer, sheet_name="活跃天数指标", index=False)
        for sheet_name, df in site_dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    logger.info("Completed: 1_指标数据")
    # Clean up
    mysql_engine.dispose()
    mongo_client.close()

if __name__ == "__main__":
    main()

