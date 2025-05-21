import os
from dateutil.relativedelta import relativedelta
import pandas as pd
import pymongo
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import sys # Import sys to get script name

# 参数配置
site_id = 1000
db_uri = "mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/"
db_name = "update_records"

# 场馆类型映射 (用于过滤，不是最终输出列的内容)
game_type_map_filter = {
    1: '体育',
    2: '电竞',
}

# 星期映射
weekday_map = {
    0: '星期一', # Monday is 0 in datetime.weekday()
    1: '星期二',
    2: '星期三',
    3: '星期四',
    4: '星期五',
    5: '星期六',
    6: '星期日'
}

# 需要合并到同一个Excel的场馆类型 (使用映射后的名称)
target_venues = ['体育', '电竞']

# 反向映射，用于根据场馆名称获取game_type
venue_name_to_type = {v: k for k, v in game_type_map_filter.items()}

def get_raw_records(start_date: str, end_date: str) -> list:
    """
    从 MongoDB 获取原始投注数据列表。
    只获取必要的字段，不进行聚合或复杂处理。
    """
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    st, et = f"{start_date} 00:00:00", f"{end_date} 23:59:59"
    # 过滤**名称，只获取需要的场馆数据
    # Using venue_name in the query is more efficient than filtering collection names
    # Assuming venue_name field exists and corresponds to DBDJ, XMTY, IMDJ
    target_venue_names_raw = ['DBDJ', 'XMTY', 'IMDJ'] # Raw venue names in DB

    cols = [c for c in db.list_collection_names() if c.startswith('pull_order')]
    recs = []

    def fetch_from_collection(coll):
        # Match on date range, site_id, flag, and raw venue names
        pipe = [
            {"$match": {
                "flag": 1,
                "site_id": site_id,
                "start_time": {"$gte": st, "$lte": et},
                "venue_name": {"$in": target_venue_names_raw} # Filter by raw venue name
            }},
            {"$project": {
                "_id": 0, # Exclude _id
                "start_time": 1,
                "game_play_info": 1,
                "play_info": 1,
                "member_id": 1,
                "valid_bet_amount": 1,
                "game_type": 1,
                "game_name": 1,
                "venue_name": 1
            }}
        ]
        try:
            # Check if collection exists and has documents before aggregating
            if coll in db.list_collection_names() and db[coll].estimated_document_count() > 0:
                 # Fetch all results into memory for this collection
                 return list(db[coll].aggregate(pipe))
            else:
                 return []
        except Exception as e:
             print(f"Error processing collection {coll} for {start_date}-{end_date}: {e}")
             return []

    # Use ThreadPoolExecutor to fetch from different collections concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Pass the date range to the worker function
        futures = [executor.submit(fetch_from_collection, coll) for coll in cols]
        for future in futures:
            try:
                 recs.extend(future.result())
            except Exception as e:
                 print(f"Error retrieving result from future: {e}")

    client.close() # Close connection after fetching
    return recs

def process_raw_record(rec: dict) -> dict:
    """
    处理单条原始记录，提取赛事名称和球队。
    """
    game_play_info = rec.get("game_play_info", "")
    play_info = rec.get("play_info", "")
    game_name = rec.get("game_name", "")
    venue_name_raw = rec.get("venue_name", "")

    lines = game_play_info.split('\n')

    # Extract event_name
    event_name = ""
    if len(lines) >= 2 and lines[1].strip() != "":
        second_line = lines[1]
        if ' - ' in second_line:
            event_name = second_line.split(' - ')[0].strip()
        else:
            event_name = second_line.strip()
    else:
        play_lines = play_info.split('\n')
        if len(play_lines) >= 2:
            league_line = play_lines[1]
            if "联赛名称:" in league_line:
                event_name = league_line.split("联赛名称:")[1].strip()
            # If no league name found in play_info, event_name remains ""

    # Extract team_name based on venue_name_raw
    team_name = ""
    if venue_name_raw in ['XMTY', 'IMDJ']:
         if len(lines) >= 3 and lines[2].strip() != "":
             team_name = lines[2].strip()
    elif venue_name_raw == 'DBDJ':
        # For DBDJ, check the second line for event name presence before trying to extract team from third line
        if len(lines) >= 2 and lines[1].strip() != "":
            if len(lines) >= 3 and lines[2].strip() != "": # Check if third line exists and is not empty
                if ' ' in lines[2]:
                     team_name = lines[2].split(' ')[0].strip()
                else:
                     team_name = lines[2].strip()
        # Fallback to game_name if team_name is still empty and game_name exists
        if not team_name and game_name:
             team_name = game_name

    event_date = rec["start_time"][:10]
    game_type = rec.get("game_type", 0)

    return {
        "赛事日期": event_date,
        "赛事名称": event_name,
        "球队": team_name,
        "game_type": game_type,
        "game_name": game_name,
        "member_id": rec.get("member_id"),
        "valid_bet_amount": rec.get("valid_bet_amount", 0)
    }


def get_dates_for_weekday_in_last_month(target_date: date) -> list[str]:
    """获取上月与target_date相同星期的所有日期字符串列表"""
    target_weekday = target_date.weekday() # 0-6

    # Calculate the first day of the previous month
    first_day_of_this_month = target_date.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)

    dates_list = []
    current_date = first_day_of_last_month
    while current_date <= last_day_of_last_month:
        if current_date.weekday() == target_weekday:
            dates_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return dates_list


if __name__ == '__main__':
    # 记录运行开始时间
    start_time = datetime.now()
    print(f"运行开始时间 {start_time.strftime('%Y-%m-%d %H:%M')}")

    today = date.today()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    yesterday_weekday = yesterday.weekday() # 0-6
    yesterday_weekday_name = weekday_map.get(yesterday_weekday, f"星期{yesterday_weekday + 1}")

    print(f"正在处理昨天 ({yesterday_str}, {yesterday_weekday_name}) 的数据...")

    # 获取昨天原始记录列表
    raw_records_yesterday = get_raw_records(yesterday_str, yesterday_str)
    # 处理昨天原始记录
    processed_records_yesterday = [process_raw_record(rec) for rec in raw_records_yesterday]
    df_yesterday_processed = pd.DataFrame(processed_records_yesterday)
    print(f"昨天原始记录获取并处理完成，共 {len(processed_records_yesterday)} 条记录。")
    del raw_records_yesterday, processed_records_yesterday # Release memory

    # 获取上月与昨天相同星期的所有日期
    last_month_same_weekday_dates = get_dates_for_weekday_in_last_month(yesterday)

    # 拉取上月同星期原始数据 (如果存在日期)，并行处理每个日期
    raw_records_last_month = []
    if last_month_same_weekday_dates:
        print(f"正在获取上月同星期 ({yesterday_weekday_name}) 的原始数据，日期范围：{min(last_month_same_weekday_dates)} 到 {max(last_month_same_weekday_dates)}")

        # Use ThreadPoolExecutor to fetch data for each date in parallel
        with ThreadPoolExecutor(max_workers=5) as executor: # Adjust max_workers as needed
            futures = [executor.submit(get_raw_records, date_str, date_str) for date_str in last_month_same_weekday_dates]
            for future in futures:
                try:
                    raw_records_last_month.extend(future.result())
                except Exception as e:
                    print(f"Error retrieving result from future for last month data: {e}")

        # Process all last month raw records
        processed_records_last_month = [process_raw_record(rec) for rec in raw_records_last_month]
        df_last_month_processed = pd.DataFrame(processed_records_last_month)
        print(f"上月同星期原始记录获取并处理完成，共 {len(processed_records_last_month)} 条记录。")
        del raw_records_last_month, processed_records_last_month # Release memory

    else:
        print(f"上月没有找到与昨天 ({yesterday_weekday_name}) 相同的星期日期。")
        df_last_month_processed = pd.DataFrame() # Empty DataFrame if no dates


    # 存储需要输出到Excel的数据，按目标场馆类型分组
    processed_data_for_excel = {} # {venue_name_str: {'latest': df, 'avg': df, 'diff': df}}


    for venue_name_str in target_venues:
        target_game_type = venue_name_to_type.get(venue_name_str)
        if target_game_type is None:
            print(f"未知的目标场馆类型: {venue_name_str}，跳过处理。")
            continue

        print(f"正在处理场馆：{venue_name_str} (game_type: {target_game_type})")

        # 过滤出当前 game_type 的数据
        venue_yesterday_processed = df_yesterday_processed[df_yesterday_processed['game_type'] == target_game_type].copy() if not df_yesterday_processed.empty else pd.DataFrame()
        venue_last_month_processed = df_last_month_processed[df_last_month_processed['game_type'] == target_game_type].copy() if not df_last_month_processed.empty else pd.DataFrame()

        # 对过滤后的数据进行聚合
        # 聚合键包含 game_name
        group_keys = ['赛事日期', '赛事名称', '球队', 'game_name']

        venue_yesterday_agg = pd.DataFrame()
        if not venue_yesterday_processed.empty:
             venue_yesterday_agg = venue_yesterday_processed.groupby(group_keys).agg(
                 有效投注=('valid_bet_amount', 'sum'),
                 投注次数=('member_id', 'count'),
                 投注人数=('member_id', 'nunique')
             ).reset_index()
        else:
             print(f"场馆 {venue_name_str} 昨天没有找到数据。")


        venue_last_month_agg = pd.DataFrame()
        if not venue_last_month_processed.empty:
             # Filter last month data to include only the specific dates (already done by get_raw_records for these dates)
             # No need to filter dates again here, just aggregate
             venue_last_month_agg = venue_last_month_processed.groupby(group_keys).agg(
                 有效投注=('valid_bet_amount', 'sum'),
                 投注次数=('member_id', 'count'),
                 投注人数=('member_id', 'nunique')
             ).reset_index()
        else:
             print(f"场馆 {venue_name_str} 上月没有找到原始数据。")


        # 计算上月同星期平均值
        avg_data = pd.DataFrame()
        if not venue_last_month_agg.empty:
            # 计算平均值时，按 赛事名称, 球队, game_name 分组
            avg_data = venue_last_month_agg.groupby(['赛事名称', '球队', 'game_name']).agg(
                有效投注=('有效投注', 'mean'),
                投注次数=('投注次数', 'mean'),
                投注人数=('投注人数', 'mean')
            ).reset_index()
        else:
            print(f"场馆 {venue_name_str} 在上月同星期聚合后没有数据，无法计算平均值。")


        # 准备最新一天数据 (昨天的)
        latest_data = venue_yesterday_agg.copy() if not venue_yesterday_agg.empty else pd.DataFrame(columns=group_keys + ['有效投注', '投注次数', '投注人数'])
        # 添加星期列 (虽然最终输出不需要，但在合并前保留)
        if not latest_data.empty:
             latest_data['星期'] = yesterday_weekday_name


        # 合并最新一天数据和上月平均值
        # 使用 outer merge 以保留所有事件
        merge_cols = ['赛事名称', '球队', 'game_name'] # 使用 game_name 作为合并键

        # 确保合并前列存在并选择需要的列
        # We only need the aggregated columns and merge keys
        latest_data_for_merge = latest_data[merge_cols + ['有效投注', '投注次数', '投注人数']].copy() if not latest_data.empty else pd.DataFrame(columns=merge_cols + ['有效投注', '投注次数', '投注人数'])
        avg_data_for_merge = avg_data[merge_cols + ['有效投注', '投注次数', '投注人数']].copy() if not avg_data.empty else pd.DataFrame(columns=merge_cols + ['有效投注', '投注次数', '投注人数'])


        merged_data = pd.merge(latest_data_for_merge, avg_data_for_merge, on=merge_cols,
                               how='outer', suffixes=('_最新', '_平均'))


        # 对有效投注、投注次数、投注人数字段的空值填充为 0
        for col_suffix in ['有效投注', '投注次数', '投注人数']:
             col_latest = f'{col_suffix}_最新'
             col_avg = f'{col_suffix}_平均'
             # Ensure columns exist before filling NaNs
             if col_latest in merged_data.columns:
                  merged_data[col_latest] = merged_data[col_latest].fillna(0)
             else:
                  merged_data[col_latest] = 0 # Add column if it doesn't exist
             if col_avg in merged_data.columns:
                  merged_data[col_avg] = merged_data[col_avg].fillna(0)
             else:
                  merged_data[col_avg] = 0 # Add column if it doesn't exist


        # 计算差值
        merged_data['有效投注差值'] = merged_data['有效投注_最新'] - merged_data['有效投注_平均']
        merged_data['投注次数差值'] = merged_data['投注次数_最新'] - merged_data['投注次数_平均']
        merged_data['投注人数差值'] = merged_data['投注人数_最新'] - merged_data['投注人数_平均']

        # --- 准备最终输出的 DataFrames ---

        # 定义最终需要的列和顺序
        latest_output_cols = ['场馆类型', '赛事名称', '球队', '投注人数', '有效投注']
        avg_output_cols = ['场馆类型', '赛事名称', '球队', '投注人数', '有效投注']
        diff_output_cols = ['场馆类型', '赛事名称', '球队', '投注人数差值', '有效投注差值']


        # 差值数据框
        # Select and rename columns for diff
        diff_data_final = merged_data[['赛事名称', '球队', 'game_name', '有效投注差值', '投注人数差值']].copy()
        diff_data_final = diff_data_final.rename(columns={'game_name': '场馆类型'}) # 重命名 game_name 列
        # Reorder columns
        diff_data_final = diff_data_final[diff_output_cols] if all(col in diff_data_final.columns for col in diff_output_cols) else pd.DataFrame(columns=diff_output_cols)


        # 上月均值数据框
        # Select and rename columns for avg
        avg_data_final = avg_data.copy() if not avg_data.empty else pd.DataFrame(columns=['赛事名称', '球队', 'game_name', '有效投注', '投注人数'])
        avg_data_final = avg_data_final.rename(columns={'game_name': '场馆类型'}) # 重命名 game_name 列
        # Reorder columns
        avg_data_final = avg_data_final[avg_output_cols] if all(col in avg_data_final.columns for col in avg_output_cols) else pd.DataFrame(columns=avg_output_cols)


        # 最新一天数据框
        # Start from venue_yesterday_agg, which has the correct aggregated columns
        latest_data_final = venue_yesterday_agg[['赛事名称', '球队', 'game_name', '有效投注', '投注人数']].copy() if not venue_yesterday_agg.empty else pd.DataFrame(columns=['赛事名称', '球队', 'game_name', '有效投注', '投注人数'])
        latest_data_final = latest_data_final.rename(columns={'game_name': '场馆类型'}) # 重命名 game_name 列
        # Reorder columns
        latest_data_final = latest_data_final[latest_output_cols] if all(col in latest_data_final.columns for col in latest_output_cols) else pd.DataFrame(columns=latest_output_cols)


        # Store the processed data for this venue
        processed_data_for_excel[venue_name_str] = {
            'latest': latest_data_final,
            'avg': avg_data_final,
            'diff': diff_data_final
        }

    # --- Writing to a single Excel file ---

    # 确保输出目录存在
    output_dir = r".\好博体育"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 获取当前脚本名称 (不带扩展名)
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    # 获取当前时间用于文件名
    current_time_str = datetime.now().strftime("%Y-%m-%d_%H.%M")
    # 创建 Excel 文件路径
    excel_filename = f"{script_name}_{current_time_str}.xlsx"
    excel_path = os.path.join(output_dir, excel_filename)


    if processed_data_for_excel: # Check if any target venue had data processed
        print(f"正在生成合并的 Excel 文件：{excel_path}")
        # 使用 ExcelWriter 创建文件
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for venue in target_venues: # Iterate through target_venues to ensure consistent sheet order
                if venue in processed_data_for_excel:
                    data = processed_data_for_excel[venue]

                    # 写入差值数据
                    diff_sheet_name = f"{venue}{yesterday_weekday_name}差值"
                    if not data['diff'].empty:
                         data['diff'].to_excel(writer, sheet_name=diff_sheet_name, index=False)
                         # 获取 openpyxl sheet 对象并设置格式
                         ws_diff = writer.sheets[diff_sheet_name]
                         ws_diff.freeze_panes = 'A2' # 冻结首行
                         ws_diff.auto_filter.ref = ws_diff.dimensions # 设置筛选
                    else:
                         print(f"场馆 {venue} 的差值数据为空，跳过写入 {diff_sheet_name} 工作表。")


                    # 写入上月平均值数据
                    avg_sheet_name = f"{venue}上月{yesterday_weekday_name}均值"
                    if not data['avg'].empty:
                         data['avg'].to_excel(writer, sheet_name=avg_sheet_name, index=False)
                         # 获取 openpyxl sheet 对象并设置格式
                         ws_avg = writer.sheets[avg_sheet_name]
                         ws_avg.freeze_panes = 'A2' # 冻结首行
                         ws_avg.auto_filter.ref = ws_avg.dimensions # 设置筛选
                    else:
                         print(f"场馆 {venue} 的上月均值数据为空，跳过写入 {avg_sheet_name} 工作表。")


                    # 写入最新一天数据
                    latest_sheet_name = f"{venue}{yesterday_weekday_name}最新"
                    if not data['latest'].empty:
                         data['latest'].to_excel(writer, sheet_name=latest_sheet_name, index=False)
                         # 获取 openpyxl sheet 对象并设置格式
                         ws_latest = writer.sheets[latest_sheet_name]
                         ws_latest.freeze_panes = 'A2' # 冻结首行
                         ws_latest.auto_filter.ref = ws_latest.dimensions # 设置筛选
                    else:
                         print(f"场馆 {venue} 的最新数据为空，跳过写入 {latest_sheet_name} 工作表。")

                else:
                    print(f"场馆 {venue} 没有找到数据，跳过写入该场馆的工作表。")

        print(f"合并的 Excel 文件生成完成：{excel_path}")
    else:
        print(f"目标场馆 ({', '.join(target_venues)}) 均没有找到数据，未生成 Excel 文件。")

    print("所有目标场馆数据处理完成。")

    # 记录运行结束时间
    end_time = datetime.now()
    print(f"运行结束时间 {end_time.strftime('%Y-%m-%d %H:%M')}")

