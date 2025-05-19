import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import traceback
import math # 导入 math 模块用于检查 NaN
import xlsxwriter # 确保安装了 xlsxwriter

class DatabaseQuery:
    def __init__(self, host: str, port: int, user: str, password: str,
                 site_id: int = 2000, start_date: str = '2024-10-01', end_date: str = '2025-04-30',
                 agent_1000: str = 'agent_1000', u1_1000: str = 'u1_1000',
                 bigdata: str = 'bigdata', control_1000: str = 'control_1000',
                 finance_1000: str = 'finance_1000'):
        """初始化数据库连接参数"""
        # MySQL 连接
        connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/"
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        # 其他参数
        self.control_1000 = control_1000
        self.bigdata = bigdata
        self.agent_1000 = agent_1000
        self.u1_1000 = u1_1000
        self.finance_1000 = finance_1000
        self.site_id = site_id
        self.start_date = start_date
        self.end_date = end_date
        # 将日期字符串转换为 datetime 对象，方便计算
        self.start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        self.end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()


    def close_connections(self):
        """关闭 MySQL 连接"""
        if self.session:
            self.session.close()

    def _0_promotion(self) -> pd.DataFrame:
        """查询推广部相关信息，包含 site_id"""
        query = f"""
       SELECT
           a1_ad.site_id AS '站点', -- 添加 site_id
           a1_ad.group_name AS '一级',
           a1_ad_2.group_name AS '二级',
           a1_ad_3.group_name AS '三级',
           a1_ad_4.group_name AS '四级',
           a1_adm.agent_name AS '代理账号',
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

        return pd.read_sql_query(query, self.engine)


    def _get_overall_first_deposit(self) -> pd.DataFrame:
        """查询会员的首次存款日期（不限时间范围）"""
        query = f"""
        SELECT
            member_id AS '会员ID',
            MIN(statics_date) AS 'first_deposit_date'
        FROM {self.bigdata}.member_daily_statics
        WHERE deposit > 0
        GROUP BY member_id
       """
        return pd.read_sql_query(query, self.engine)

    def _get_betting_days_in_period(self) -> pd.DataFrame:
        """查询会员在指定时间段内有投注的日期"""
        query = f"""
        SELECT
            member_id AS '会员ID',
            statics_date AS 'betting_date'
        FROM {self.bigdata}.member_daily_statics
        WHERE statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
          AND bets > 0
        """
        return pd.read_sql_query(query, self.engine)


def save_to_excel(dataframes: dict[str, pd.DataFrame], filename: str):
    """保存多个 DataFrame 到 Excel 文件的不同 Sheet，按指定顺序"""
    # 定义 Sheet 的顺序
    sheet_order = ['1级', '2级', '3级', '4级', '代理']

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        for sheet_name in sheet_order:
            if sheet_name in dataframes:
                df = dataframes[sheet_name]
                # 确保 '会员ID' 列不在最终输出中 (虽然汇总后应该没有，但以防万一)
                df_to_save = df.drop(columns=['会员ID'], errors='ignore')
                df_to_save.to_excel(writer, sheet_name=sheet_name, index=False)
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]
                # 防止空 DataFrame 导致冻结和过滤错误
                if not df_to_save.empty:
                    worksheet.freeze_panes(1, 0)
                    worksheet.autofilter(0, 0, 0, len(df_to_save.columns) - 1)
            else:
                print(f"警告: 未找到 Sheet '{sheet_name}' 对应的数据。")


def work(db_query: DatabaseQuery) -> dict[str, pd.DataFrame]:
    """执行查询并计算指标，返回按层级分组的 DataFrame 字典"""
    print("正在查询推广架构...")
    df_promo = db_query._0_promotion()

    print("正在查询会员首存日期...")
    df_first_dep = db_query._get_overall_first_deposit()

    print("正在查询指定日期范围内的投注日期...")
    df_betting = db_query._get_betting_days_in_period()

    # 将日期列转换为 datetime 对象，方便后续计算
    df_first_dep['first_deposit_date'] = pd.to_datetime(df_first_dep['first_deposit_date']).dt.date
    df_betting['betting_date'] = pd.to_datetime(df_betting['betting_date']).dt.date

    # 合并推广架构和首存日期
    df_member_info = df_promo.merge(df_first_dep, on='会员ID', how='left')

    # 将投注日期按会员ID聚合为集合，方便查找
    df_betting_sets = df_betting.groupby('会员ID')['betting_date'].apply(set).reset_index(name='betting_days_set')

    # 合并会员信息和投注日期集合
    df_member_stats = df_member_info.merge(df_betting_sets, on='会员ID', how='left')

    # 处理没有投注记录的会员，其 betting_days_set 将是 NaN，替换为空集合
    df_member_stats['betting_days_set'] = df_member_stats['betting_days_set'].apply(lambda x: x if isinstance(x, set) else set())

    # --- 计算每位会员的指标相关的布尔值或计数 ---

    # 首存人数（在报表日期范围内首存的会员）
    df_member_stats['is_fd_in_range'] = df_member_stats['first_deposit_date'].between(db_query.start_date_dt, db_query.end_date_dt)

    # 一次性人数（在报表日期范围内首存，且在首存日期后，报表日期范围内只有1天有投注）
    def count_betting_days_after_fd_in_range(row):
        if pd.isna(row['first_deposit_date']):
            return 0
        # 过滤出在首存日期之后且在报表日期范围内的投注日期
        betting_days_after = {d for d in row['betting_days_set'] if d > row['first_deposit_date'] and db_query.start_date_dt <= d <= db_query.end_date_dt}
        return len(betting_days_after)

    df_member_stats['betting_days_after_fd_in_range_count'] = df_member_stats.apply(count_betting_days_after_fd_in_range, axis=1)
    df_member_stats['is_onetime'] = df_member_stats['is_fd_in_range'] & (df_member_stats['betting_days_after_fd_in_range_count'] == 1)

    # 计算 N 日留存相关指标
    retention_days = [2, 3, 5, 7, 10, 15]
    for n in retention_days:
        # N日首存（在报表日期范围的 N 日窗口内首存的会员）
        # 窗口结束日期 = 报表结束日期 - (N-1) 天
        window_end_date = db_query.end_date_dt - timedelta(days=n-1)
        # 确保窗口结束日期不早于报表开始日期
        window_end_date = max(window_end_date, db_query.start_date_dt)

        df_member_stats[f'is_fd_in_{n}_day_window'] = df_member_stats['first_deposit_date'].between(db_query.start_date_dt, window_end_date)

        # N日留存（在 N 日窗口内首存，并在首存后第 N 天有投注的会员）
        def has_bet_on_day_n(row, n_days):
            if not row[f'is_fd_in_{n_days}_day_window']:
                return False
            # 计算留存日期
            retention_date = row['first_deposit_date'] + timedelta(days=n_days-1)
            # 留存日期必须在报表日期范围内才能算留存
            if not (db_query.start_date_dt <= retention_date <= db_query.end_date_dt):
                 return False
            return retention_date in row['betting_days_set']

        df_member_stats[f'has_bet_on_day_{n}'] = df_member_stats.apply(lambda row: has_bet_on_day_n(row, n), axis=1)


    # --- 按不同层级分组并聚合计算总数 ---

    # 定义需要聚合的计数列 (原始布尔列名)
    count_cols_bool = ['is_fd_in_range', 'is_onetime'] + [f'is_fd_in_{n}_day_window' for n in retention_days] + [f'has_bet_on_day_{n}' for n in retention_days]

    # 定义最终输出的指标列名 (计算后的列名)
    metric_cols = [
        '首存人数', '一次性人数', '一次性占比',
    ]
    for n in retention_days:
        metric_cols.extend([f'{n}日首存', f'{n}日留存', f'{n}日留存率'])


    # 函数：聚合数据并计算比率
    def aggregate_and_calculate_ratios(df, group_cols):
        # 选择需要聚合的列，包括分组列和计数列
        cols_to_agg = group_cols + count_cols_bool
        # 过滤掉不在 df 中的列
        cols_to_agg = [col for col in cols_to_agg if col in df.columns]

        # 按指定列分组并对计数列求和 (summing booleans results in integers)
        # 使用 dropna=False 保留所有组合，即使某些层级下没有会员
        agg_df = df[cols_to_agg].groupby(group_cols, dropna=False).sum().reset_index()

        # 重命名计数列
        rename_map = {
            'is_fd_in_range': '首存人数',
            'is_onetime': '一次性人数',
        }
        for n in retention_days:
            rename_map[f'is_fd_in_{n}_day_window'] = f'{n}日首存'
            rename_map[f'has_bet_on_day_{n}'] = f'{n}日留存'
        agg_df = agg_df.rename(columns=rename_map)

        # --- 将计数列转换为整数类型 ---
        count_cols_renamed = ['首存人数', '一次性人数'] + [f'{n}日首存' for n in retention_days] + [f'{n}日留存' for n in retention_days]
        for col in count_cols_renamed:
            if col in agg_df.columns: # 检查列是否存在
                 # 使用 errors='coerce' 可以将无法转换的值变为 NaN，然后用 fillna(0) 填充
                 agg_df[col] = pd.to_numeric(agg_df[col], errors='coerce').fillna(0).astype(int)
        # --- 结束类型转换 ---


        # 计算比率 (这些列现在是整数类型)
        # 使用 .loc 避免 SettingWithCopyWarning
        # 避免除以零，并处理 NaN/Inf
        agg_df.loc[:, '一次性占比'] = (agg_df['一次性人数'] / agg_df['首存人数']).fillna(0).replace([float('inf'), -float('inf')], 0).round(4)

        for n in retention_days:
            col_首存 = f'{n}日首存'
            col_留存 = f'{n}日留存'
            col_留存率 = f'{n}日留存率'
            if col_首存 in agg_df.columns and col_留存 in agg_df.columns:
                 # 避免除以零，并处理 NaN/Inf
                 agg_df.loc[:, col_留存率] = (agg_df[col_留存] / agg_df[col_首存]).fillna(0).replace([float('inf'), -float('inf')], 0).round(4)
            else:
                 agg_df.loc[:, col_留存率] = 0.0


        # 定义该层级最终需要的列顺序
        # group_cols 已经包含了层级列和代理账号列 (如果需要的话)
        level_final_cols = group_cols + metric_cols

        # 确保所有需要的列都存在，并填充默认值
        for col in level_final_cols:
            if col not in agg_df.columns:
                 if any(metric in col for metric in ['人数', '首存', '留存']):
                      agg_df[col] = 0
                 elif '占比' in col or '留存率' in col:
                      agg_df[col] = 0.0
                 else:
                      agg_df[col] = ''

        # 重新排序和选择列
        agg_df = agg_df[level_final_cols]

        return agg_df


    # 生成各层级的数据
    dataframes = {}

    # 1级层级
    dataframes['1级'] = aggregate_and_calculate_ratios(df_member_stats, ['站点', '一级'])

    # 2级层级
    dataframes['2级'] = aggregate_and_calculate_ratios(df_member_stats, ['站点', '一级', '二级'])

    # 3级层级
    dataframes['3级'] = aggregate_and_calculate_ratios(df_member_stats, ['站点', '一级', '二级', '三级'])

    # 4级层级
    dataframes['4级'] = aggregate_and_calculate_ratios(df_member_stats, ['站点', '一级', '二级', '三级', '四级'])

    # 代理层级 (按代理账号汇总)
    dataframes['代理'] = aggregate_and_calculate_ratios(df_member_stats, ['站点', '一级', '二级', '三级', '四级', '代理账号'])


    return dataframes


def main():
    start_time = datetime.now()
    print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")
    db_query = DatabaseQuery(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        # site_id=1000,
        # start_date='2025-04-01',
        # end_date='2025-04-30',
    )
    try:
        # work 函数现在返回一个字典
        result_dataframes = work(db_query)
        if db_query.start_date == db_query.end_date:
            start_dt = datetime.strptime(db_query.start_date, '%Y-%m-%d')
            date_str = f"{start_dt.month}-{start_dt.day}"
        else:
            start_dt = datetime.strptime(db_query.start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(db_query.end_date, '%Y-%m-%d')
            date_str = f"{start_dt.month}-{start_dt.day}_{end_dt.month}-{end_dt.day}"

        site_str = db_query.site_id if db_query.site_id is not None else 'ALL'
        excel_filename = f"【{site_str}_{date_str}】推广首留存 {start_time.strftime('%m-%d_%H.%M')}.xlsx"
        # 将字典传递给 save_to_excel
        save_to_excel(result_dataframes, excel_filename)
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
