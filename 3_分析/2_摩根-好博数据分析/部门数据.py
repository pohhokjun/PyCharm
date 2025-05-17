import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
import pymongo
from concurrent.futures import ThreadPoolExecutor

# ------------------- 配置 -------------------
MYSQL_URI    = 'mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/bigdata'
TABLE_NAME   = 'platform_daily_report'
OUTPUT_PATH  = 'output'
site_id      = 1000

# ------------------- 日期计算 -------------------
yesterday                     = datetime.now().date() - timedelta(days=1)
first_day_current_month       = yesterday.replace(day=1)
first_day_previous_month      = first_day_current_month - relativedelta(months=1)
last_day_previous_month       = first_day_current_month - timedelta(days=1)

first_day_current_month_start = first_day_current_month.strftime('%Y-%m-%d')
yesterday_end                 = yesterday.strftime('%Y-%m-%d')
first_day_previous_month_start = first_day_previous_month.strftime('%Y-%m-%d')
last_day_previous_month_end   = last_day_previous_month.strftime('%Y-%m-%d')

# ------------------- 数据查询函数 -------------------
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

    numeric_cols = ['首存人数','存款人数','存款金额','投注人数','有效投注']

    # 1️⃣ “官资部” 全部日期聚合
    df_office = (
        df[df['1级']=='官资部']
        .groupby('日期', as_index=False)[numeric_cols].sum()
    )
    df_office['部门'] = '官资部'

    # 2️⃣ “推广部” 保留指定子部门
    promote_keep = {'推广1部','推广3部','推广5部','推广6部','推广7部','推广9部','推广11部','推广12部'}
    df_promote = df[
        (df['1级']=='推广部') & df['部门'].isin(promote_keep)
    ][['日期','部门']+numeric_cols].copy()

    # # 3️⃣ “招商部”：先取指定，然后其余合并为“招商部其他”
    # invest_keep = {'招商1部','招商2部','招商5部'}
    # df_inv = df[df['1级']=='招商部'].copy()
    # df_inv_spec = df_inv[df_inv['部门'].isin(invest_keep)][['日期','部门']+numeric_cols]
    # df_inv_other = (
    #     df_inv[~df_inv['部门'].isin(invest_keep)]
    #     .groupby('日期', as_index=False)[numeric_cols].sum()
    # )
    # df_inv_other['部门'] = '招商部其他'
    #
    # df_invest_final = pd.concat([df_inv_spec, df_inv_other], ignore_index=True)
    # 3️⃣ “招商部” 全部日期聚合
    df_invest = (
        df[df['1级'] == '招商部']
        .groupby('日期', as_index=False)[numeric_cols].sum()
    )
    df_invest['部门'] = '招商部'

    # 4️⃣ 合并三部分
    df_result = pd.concat([df_office, df_promote, df_invest], ignore_index=True)
    return df_result[['部门','日期']+numeric_cols]

# ------------------- 并行执行 & 合并 -------------------
if __name__ == '__main__':
    # 并行拉取两张表
    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(member_type)
        f2 = executor.submit(department_data)
        df_member = f1.result()
        df_dept   = f2.result()

    # 合并会员和部门两张表
    df_merged = pd.concat([df_member, df_dept], ignore_index=True)
    # 1️⃣ 把“部门”列放到“日期”列前面
    cols = df_merged.columns.tolist()
    cols.remove('部门')
    cols.remove('日期')
    df_merged = df_merged[['部门', '日期'] + cols]

    # 2️⃣ 按“部门”、“日期”分组，数值列求和
    numeric_cols = [c for c in df_merged.columns if c not in ['部门', '日期']]
    final = (
        df_merged
        .groupby(['部门', '日期'], as_index=False)[numeric_cols]
        .sum()
    )

    # 4️⃣ 确保“日期”是 datetime 类型
    final['日期'] = pd.to_datetime(final['日期'])

    # 5️⃣ 添加“星期”列（放在“有效投注”后面，值为1–7）
    final['星期'] = final['日期'].dt.dayofweek + 1
    idx = final.columns.get_loc('有效投注') + 1
    final.insert(idx, '星期', final.pop('星期'))

    # 添加辅助列：月、日
    final['月'] = final['日期'].dt.month
    # 定义部门排序键：其他部门=0，直客=1000，普代=1001，官代=1002
    dept_special = {'直客': 1000, '普代': 1001, '官代': 1002}
    final['部门排序'] = final['部门'].apply(lambda x: dept_special.get(x, 0))

    # 3️⃣ 按月降序、部门排序升序、日升序
    final = final.sort_values(
        by=['月', '部门排序'],
        ascending=[False, True]
    )

    # 4️⃣ 将“日期”格式化为 “YYYY-MM-DD” (在排序后)
    final['日期'] = final['日期'].dt.strftime('%Y-%m-%d')

    # 删除辅助列
    final = final.drop(columns=['月', '部门排序'])

    # 设置输出文件路径并保存
    final.to_excel(
        fr".\好博体育\部门数据.xlsx",
        index=False,
        engine="openpyxl",
    )
    print(f"完成：已输出")
