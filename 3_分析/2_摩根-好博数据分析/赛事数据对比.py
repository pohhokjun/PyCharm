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

# 场馆类型映射
game_type_map = {
    1: '体育', 2: '电竞', 3: '真人',
    4: '彩票', 5: '棋牌', 6: '电子', 7: '捕鱼'
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

# 需要合并到同一个Excel的场馆类型
target_venues = ['体育', '电竞']


def event_data(start_date: str, end_date: str) -> pd.DataFrame:
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    st, et = f"{start_date} 00:00:00", f"{end_date} 23:59:59"
    cols = [c for c in db.list_collection_names() if
            c.startswith('pull_order') and (c.endswith('DBDJ') or c.endswith('XMTY') or c.endswith('IMDJ'))]
    recs = []

    def process_collection(coll):
        pipe = [
            {"$match": {"flag": 1, "site_id": site_id, "start_time": {"$gte": st, "$lte": et}}},
            {"$project": {
                "start_time": 1,
                "game_play_info": 1,
                "play_info": 1,
                "member_id": 1,
                "valid_bet_amount": 1,
                "game_type": 1,
                "game_name": 1
            }}
        ]
        # Ensure collection exists and is not empty before aggregating
        try:
            if coll in db.list_collection_names() and db[coll].estimated_document_count() > 0:
                 return [{"record": rec, "coll": coll} for rec in db[coll].aggregate(pipe)]
            else:
                 return []
        except Exception as e:
             print(f"Error processing collection {coll}: {e}")
             return []


    # Use ThreadPoolExecutor for fetching data from multiple collections
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_collection, coll) for coll in cols]
        for future in futures:
            try:
                 recs.extend(future.result())
            except Exception as e:
                 print(f"Error retrieving result from future: {e}")


    data = []
    for item in recs:
        rec = item["record"]
        coll = item["coll"]
        game_play_info = rec.get("game_play_info", "")
        play_info = rec.get("play_info", "")
        game_name = rec.get("game_name", "")
        coll_suffix = coll.split('_')[-1]

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

        # Extract team_name
        team_name = ""
        if coll_suffix in ['XMTY', 'IMDJ']:
             if len(lines) >= 3 and lines[2].strip() != "":
                 team_name = lines[2].strip()
        elif coll_suffix == 'DBDJ':
            # For DBDJ, check the second line for event name presence before trying to extract team from third line
            if len(lines) >= 2 and lines[1].strip() != "":
                if len(lines) >= 3 and lines[2].strip() != "": # Check if third line exists and is not empty
                    if ' ' in lines[2]:
                         team_name = lines[2].split(' ')[0].strip()
                    else:
                         team_name = lines[2].strip()
            if not team_name and game_name: # Fallback to game_name if team_name is still empty
                 team_name = game_name


        event_date = rec["start_time"][:10]
        game_type = rec.get("game_type", 0)
        venue_type = game_type_map.get(game_type, "未知")
        data.append({
            "赛事日期": event_date,
            "赛事名称": event_name,
            "球队": team_name,
            "场馆类型": venue_type,
            "member_id": rec.get("member_id"), # Use .get for safety
            "valid_bet_amount": rec.get("valid_bet_amount", 0) # Use .get for safety, default 0
        })

    df = pd.DataFrame(data)

    # Grouping and aggregation
    if not df.empty:
        grouped = df.groupby(['赛事日期', '赛事名称', '球队', '场馆类型'])
        df_result = grouped.agg({
            'valid_bet_amount': 'sum',
            'member_id': [('投注次数', 'count'), ('投注人数', lambda x: x.nunique())]
        }).reset_index()
        # Flatten multi-level columns after aggregation
        df_result.columns = ['赛事日期', '赛事名称', '球队', '场馆类型', '有效投注', '投注次数', '投注人数']
    else:
        # Return an empty DataFrame with the correct columns if no data is found
        df_result = pd.DataFrame(columns=['赛事日期', '赛事名称', '球队', '场馆类型', '有效投注', '投注次数', '投注人数'])

    return df_result


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
    today = date.today()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    yesterday_weekday = yesterday.weekday() # 0-6
    yesterday_weekday_name = weekday_map.get(yesterday_weekday, f"星期{yesterday_weekday + 1}")

    print(f"正在处理昨天 ({yesterday_str}, {yesterday_weekday_name}) 的数据...")

    # 获取昨天的数据
    df_yesterday = event_data(yesterday_str, yesterday_str)

    # 获取上月与昨天相同星期的所有日期
    last_month_same_weekday_dates = get_dates_for_weekday_in_last_month(yesterday)

    # 拉取上月同星期的数据 (如果存在日期)
    df_last_month_weekday = pd.DataFrame()
    if last_month_same_weekday_dates:
        print(f"正在获取上月同星期 ({yesterday_weekday_name}) 的数据，日期范围：{min(last_month_same_weekday_dates)} 到 {max(last_month_same_weekday_dates)}")
        # Fetch data for the range covering these dates.
        min_date_str = min(last_month_same_weekday_dates)
        max_date_str = max(last_month_same_weekday_dates)
        df_last_month_raw = event_data(min_date_str, max_date_str)

        # Filter the raw data to keep only the specific dates
        df_last_month_weekday = df_last_month_raw[df_last_month_raw['赛事日期'].isin(last_month_same_weekday_dates)].copy()
    else:
        print(f"上月没有找到与昨天 ({yesterday_weekday_name}) 相同的星期日期。")

    # 存储需要输出到Excel的数据，按场馆类型分组
    processed_data_for_excel = {} # {venue: {'latest': df, 'avg': df, 'diff': df}}

    # 获取所有实际存在的场馆类型，以便遍历
    all_venues_in_data = pd.concat([df_yesterday['场馆类型'], df_last_month_weekday['场馆类型']]).unique() if not df_yesterday.empty or not df_last_month_weekday.empty else []


    for venue in all_venues_in_data:
        # 只处理目标场馆类型
        if venue not in target_venues:
            continue

        print(f"正在处理场馆：{venue}")
        # 过滤出当前场馆的数据
        venue_yesterday_data = df_yesterday[df_yesterday['场馆类型'] == venue].copy() if not df_yesterday.empty else pd.DataFrame()
        venue_last_month_weekday_data = df_last_month_weekday[df_last_month_weekday['场馆类型'] == venue].copy() if not df_last_month_weekday.empty else pd.DataFrame()

        # 计算上月同星期平均值
        avg_data = pd.DataFrame()
        if not venue_last_month_weekday_data.empty:
            # Corrected aggregation: calculate mean of the already aggregated columns
            avg_data = venue_last_month_weekday_data.groupby(['赛事名称', '球队', '场馆类型']).agg({
                '有效投注': 'mean',
                '投注次数': 'mean',
                '投注人数': 'mean'
            }).reset_index()
        else:
            print(f"场馆 {venue} 在上月同星期没有找到数据，无法计算平均值。")


        # 准备最新一天数据 (昨天的)
        latest_data = venue_yesterday_data[['赛事日期', '场馆类型', '赛事名称', '球队', '有效投注', '投注次数', '投注人数']].copy() if not venue_yesterday_data.empty else pd.DataFrame(columns=['赛事日期', '场馆类型', '赛事名称', '球队', '有效投注', '投注次数', '投注人数'])
        # Add weekday name for display
        if not latest_data.empty:
             latest_data['星期'] = yesterday_weekday_name


        # 合并最新一天数据和上月平均值
        # Use outer merge to keep all events from both latest day and last month average
        # Ensure both dataframes have the merge columns even if empty
        merge_cols = ['赛事名称', '球队', '场馆类型']
        # Select only necessary columns for merge to avoid duplicate columns before suffixes
        latest_data_for_merge = latest_data[merge_cols + ['有效投注', '投注次数', '投注人数']].copy() if not latest_data.empty else pd.DataFrame(columns=merge_cols + ['有效投注', '投注次数', '投注人数'])
        avg_data_for_merge = avg_data[merge_cols + ['有效投注', '投注次数', '投注人数']].copy() if not avg_data.empty else pd.DataFrame(columns=merge_cols + ['有效投注', '投注次数', '投注人数'])


        merged_data = pd.merge(latest_data_for_merge, avg_data_for_merge, on=merge_cols,
                               how='outer', suffixes=('_最新', '_平均'))


        # 对有效投注、投注次数、投注人数字段的空值填充为 0
        for col_suffix in ['有效投注', '投注次数', '投注人数']:
             col_latest = f'{col_suffix}_最新'
             col_avg = f'{col_suffix}_平均'
             # Ensure columns exist before filling NaNs or adding
             if col_latest not in merged_data.columns:
                  merged_data[col_latest] = 0
             if col_avg not in merged_data.columns:
                  merged_data[col_avg] = 0

             merged_data[col_latest] = merged_data[col_latest].fillna(0)
             merged_data[col_avg] = merged_data[col_avg].fillna(0)


        # 计算差值
        merged_data['有效投注差值'] = merged_data['有效投注_最新'] - merged_data['有效投注_平均']
        merged_data['投注次数差值'] = merged_data['投注次数_最新'] - merged_data['投注次数_平均']
        merged_data['投注人数差值'] = merged_data['投注人数_最新'] - merged_data['投注人数_平均']

        # 准备差值数据框
        diff_data = merged_data[['赛事名称', '球队', '场馆类型', '有效投注差值', '投注次数差值', '投注人数差值']].copy()
        diff_data['赛事日期'] = f"同比上月{yesterday_weekday_name}差值"
        diff_data['星期'] = yesterday_weekday_name

        # 准备上月均值数据框
        # Need to add back the '赛事日期' and '星期' columns for the average sheet
        avg_data_for_sheet = avg_data.copy() if not avg_data.empty else pd.DataFrame(columns=['赛事名称', '球队', '场馆类型', '有效投注', '投注次数', '投注人数'])
        avg_data_for_sheet['赛事日期'] = f"上月{yesterday_weekday_name}均值"
        avg_data_for_sheet['星期'] = yesterday_weekday_name


        # 重新排列列顺序以便输出
        latest_cols = ['赛事日期', '星期', '场馆类型', '赛事名称', '球队', '有效投注', '投注次数', '投注人数']
        avg_cols_for_sheet = ['赛事日期', '星期', '场馆类型', '赛事名称', '球队', '有效投注', '投注次数', '投注人数']
        diff_cols = ['赛事日期', '星期', '场馆类型', '赛事名称', '球队', '有效投注差值', '投注次数差值', '投注人数差值']

        # Ensure columns exist and select them for final output dataframes
        latest_data_output = latest_data[latest_cols] if not latest_data.empty and all(col in latest_data.columns for col in latest_cols) else pd.DataFrame(columns=latest_cols)
        avg_data_for_sheet_output = avg_data_for_sheet[avg_cols_for_sheet] if not avg_data_for_sheet.empty and all(col in avg_cols_for_sheet for col in avg_cols_for_sheet) else pd.DataFrame(columns=avg_cols_for_sheet)
        diff_data_output = diff_data[diff_cols] if not diff_data.empty and all(col in diff_cols for col in diff_cols) else pd.DataFrame(columns=diff_cols)

        # Store the processed data for this venue
        processed_data_for_excel[venue] = {
            'latest': latest_data_output,
            'avg': avg_data_for_sheet_output,
            'diff': diff_data_output
        }

    # --- Writing to a single Excel file ---

    # 确保输出目录存在
    output_dir = r".\好博体育"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 创建 Excel 文件路径 (使用一个固定的文件名)
    excel_filename = f"体育电竞赛事数据源_{yesterday_weekday_name}.xlsx"
    excel_path = os.path.join(output_dir, excel_filename)

    if processed_data_for_excel: # Check if any target venue had data processed
        print(f"正在生成合并的 Excel 文件：{excel_path}")
        # 使用 ExcelWriter 创建文件
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for venue in target_venues:
                if venue in processed_data_for_excel:
                    data = processed_data_for_excel[venue]

                    # 写入差值数据
                    diff_sheet_name = f"{venue}{yesterday_weekday_name}差值"
                    data['diff'].to_excel(writer, sheet_name=diff_sheet_name, index=False)

                    # 写入上月平均值数据
                    avg_sheet_name = f"{venue}上月{yesterday_weekday_name}均值"
                    data['avg'].to_excel(writer, sheet_name=avg_sheet_name, index=False)

                    # 写入最新一天数据
                    latest_sheet_name = f"{venue}{yesterday_weekday_name}最新"
                    data['latest'].to_excel(writer, sheet_name=latest_sheet_name, index=False)
                else:
                    print(f"场馆 {venue} 没有找到数据，跳过写入该场馆的工作表。")

        print(f"合并的 Excel 文件生成完成：{excel_path}")
    else:
        print(f"目标场馆 ({', '.join(target_venues)}) 均没有找到数据，未生成 Excel 文件。")

    print("所有目标场馆数据处理完成。")
