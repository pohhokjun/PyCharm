
import os
from dateutil.relativedelta import relativedelta
import pandas as pd
import pymongo
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import sys

# 参数配置（保持不变）
site_id = 1000
db_uri = "mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/"
db_name = "update_records"

game_type_map_filter = {
    1: '体育',
    2: '电竞',
}

weekday_map = {
    0: '星期一',
    1: '星期二',
    2: '星期三',
    3: '星期四',
    4: '星期五',
    5: '星期六',
    6: '星期日'
}

target_venues = ['体育', '电竞']
venue_name_to_type = {v: k for k, v in game_type_map_filter.items()}

# get_raw_records 和 process_raw_record 函数保持不变
def get_raw_records(start_date: str, end_date: str) -> list:
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    st, et = f"{start_date} 00:00:00", f"{end_date} 23:59:59"
    target_venue_names_raw = ['DBDJ', 'XMTY', 'IMDJ']
    cols = [c for c in db.list_collection_names() if c.startswith('pull_order')]
    recs = []

    def fetch_from_collection(coll):
        pipe = [
            {"$match": {
                "flag": 1,
                "site_id": site_id,
                "start_time": {"$gte": st, "$lte": et},
                "venue_name": {"$in": target_venue_names_raw}
            }},
            {"$project": {
                "_id": 0,
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
            if coll in db.list_collection_names() and db[coll].estimated_document_count() > 0:
                return list(db[coll].aggregate(pipe))
            else:
                return []
        except Exception as e:
            print(f"Error processing collection {coll} for {start_date}-{end_date}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_from_collection, coll) for coll in cols]
        for future in futures:
            try:
                recs.extend(future.result())
            except Exception as e:
                print(f"Error retrieving result from future: {e}")

    client.close()
    return recs

def process_raw_record(rec: dict) -> dict:
    game_play_info = rec.get("game_play_info", "")
    play_info = rec.get("play_info", "")
    game_name = rec.get("game_name", "")
    venue_name_raw = rec.get("venue_name", "")

    lines = game_play_info.split('\n')
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

    team_name = ""
    if venue_name_raw in ['XMTY', 'IMDJ']:
        if len(lines) >= 3 and lines[2].strip() != "":
            team_name = lines[2].strip()
    elif venue_name_raw == 'DBDJ':
        if len(lines) >= 2 and lines[1].strip() != "":
            if len(lines) >= 3 and lines[2].strip() != "":
                if ' ' in lines[2]:
                    team_name = lines[2].split(' ')[0].strip()
                else:
                    team_name = lines[2].strip()
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
    target_weekday = target_date.weekday()
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
    start_time = datetime.now()
    print(f"运行开始时间 {start_time.strftime('%Y-%m-%d %H:%M')}")

    today = date.today()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    yesterday_weekday = yesterday.weekday()
    yesterday_weekday_name = weekday_map.get(yesterday_weekday, f"星期{yesterday_weekday + 1}")

    print(f"正在处理昨天 ({yesterday_str}, {yesterday_weekday_name}) 的数据...")

    raw_records_yesterday = get_raw_records(yesterday_str, yesterday_str)
    processed_records_yesterday = [process_raw_record(rec) for rec in raw_records_yesterday]
    df_yesterday_processed = pd.DataFrame(processed_records_yesterday)
    print(f"昨天原始记录获取并处理完成，共 {len(processed_records_yesterday)} 条记录。")
    del raw_records_yesterday, processed_records_yesterday

    last_month_same_weekday_dates = get_dates_for_weekday_in_last_month(yesterday)
    raw_records_last_month = []
    if last_month_same_weekday_dates:
        print(f"正在获取上月同星期 ({yesterday_weekday_name}) 的原始数据，日期范围：{min(last_month_same_weekday_dates)} 到 {max(last_month_same_weekday_dates)}")
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(get_raw_records, date_str, date_str) for date_str in last_month_same_weekday_dates]
            for future in futures:
                try:
                    raw_records_last_month.extend(future.result())
                except Exception as e:
                    print(f"Error retrieving result from future for last month data: {e}")

        processed_records_last_month = [process_raw_record(rec) for rec in raw_records_last_month]
        df_last_month_processed = pd.DataFrame(processed_records_last_month)
        print(f"上月同星期原始记录获取并处理完成，共 {len(processed_records_last_month)} 条记录。")
        del raw_records_last_month, processed_records_last_month
    else:
        print(f"上月没有找到与昨天 ({yesterday_weekday_name}) 相同的星期日期。")
        df_last_month_processed = pd.DataFrame()

    combined_data = pd.DataFrame()
    for venue_name_str in target_venues:
        target_game_type = venue_name_to_type.get(venue_name_str)
        if target_game_type is None:
            print(f"未知的目标场馆类型: {venue_name_str}，跳过处理。")
            continue

        print(f"正在处理场馆：{venue_name_str} (game_type: {target_game_type})")

        venue_yesterday_processed = df_yesterday_processed[df_yesterday_processed['game_type'] == target_game_type].copy() if not df_yesterday_processed.empty else pd.DataFrame()
        venue_last_month_processed = df_last_month_processed[df_last_month_processed['game_type'] == target_game_type].copy() if not df_last_month_processed.empty else pd.DataFrame()

        group_keys = ['赛事日期', '赛事名称', '球队', 'game_name']
        venue_yesterday_agg = pd.DataFrame()
        if not venue_yesterday_processed.empty:
            venue_yesterday_agg = venue_yesterday_processed.groupby(group_keys).agg(
                有效投注=('valid_bet_amount', 'sum'),
                投注次数=('member_id', 'count'),
                投注人数=('member_id', 'nunique')
            ).reset_index()
            venue_yesterday_agg['赛事数量'] = 1
        else:
            print(f"场馆 {venue_name_str} 昨天没有找到数据。")

        venue_last_month_agg = pd.DataFrame()
        if not venue_last_month_processed.empty:
            venue_last_month_agg = venue_last_month_processed.groupby(group_keys).agg(
                有效投注=('valid_bet_amount', 'sum'),
                投注次数=('member_id', 'count'),
                投注人数=('member_id', 'nunique')
            ).reset_index()
            venue_last_month_agg['赛事数量'] = 1
        else:
            print(f"场馆 {venue_name_str} 上月没有找到原始数据。")

        avg_data = pd.DataFrame()
        if not venue_last_month_agg.empty:
            avg_data = venue_last_month_agg.groupby(['赛事名称', '球队', 'game_name']).agg(
                有效投注=('有效投注', 'mean'),
                投注次数=('投注次数', 'mean'),
                投注人数=('投注人数', 'mean'),
                赛事数量=('赛事数量', 'sum')
            ).reset_index()
        else:
            print(f"场馆 {venue_name_str} 在上月同星期聚合后没有数据，无法计算平均值。")

        latest_data = venue_yesterday_agg.copy() if not venue_yesterday_agg.empty else pd.DataFrame(columns=group_keys + ['有效投注', '投注次数', '投注人数', '赛事数量'])
        if not latest_data.empty:
            latest_data['星期'] = yesterday_weekday_name

        merge_cols = ['赛事名称', '球队', 'game_name']
        latest_data_for_merge = latest_data[merge_cols + ['有效投注', '投注次数', '投注人数', '赛事数量']].copy() if not latest_data.empty else pd.DataFrame(columns=merge_cols + ['有效投注', '投注次数', '投注人数', '赛事数量'])
        avg_data_for_merge = avg_data[merge_cols + ['有效投注', '投注次数', '投注人数', '赛事数量']].copy() if not avg_data.empty else pd.DataFrame(columns=merge_cols + ['有效投注', '投注次数', '投注人数', '赛事数量'])

        merged_data = pd.merge(latest_data_for_merge, avg_data_for_merge, on=merge_cols,
                               how='outer', suffixes=('_最新', '_平均'))

        for col_suffix in ['有效投注', '投注次数', '投注人数', '赛事数量']:
            col_latest = f'{col_suffix}_最新'
            col_avg = f'{col_suffix}_平均'
            if col_latest in merged_data.columns:
                merged_data[col_latest] = merged_data[col_latest].fillna(0)
            else:
                merged_data[col_latest] = 0
            if col_avg in merged_data.columns:
                merged_data[col_avg] = merged_data[col_avg].fillna(0)
            else:
                merged_data[col_avg] = 0

        merged_data['有效投注差值'] = merged_data['有效投注_最新'] - merged_data['有效投注_平均']
        merged_data['投注次数差值'] = merged_data['投注次数_最新'] - merged_data['投注次数_平均']
        merged_data['投注人数差值'] = merged_data['投注人数_最新'] - merged_data['投注人数_平均']
        merged_data['赛事数量差值'] = merged_data['赛事数量_最新'] - merged_data['赛事数量_平均']

        merged_data['场馆类型'] = venue_name_str
        merged_data['游戏'] = merged_data['game_name']  # 使用 game_name 作为游戏列
        merged_data = merged_data.rename(columns={'game_name': '场馆类型_原始'})
        final_cols = ['场馆类型', '游戏', '赛事名称', '球队', '赛事数量_平均', '有效投注_平均', '投注人数_平均', '赛事数量_最新', '有效投注_最新', '投注人数_最新', '赛事数量差值', '有效投注差值', '投注人数差值']
        merged_data = merged_data[final_cols]

        combined_data = pd.concat([combined_data, merged_data], ignore_index=True)

    output_dir = r"C:\Users\Administrator\Desktop\PyCharm\3_分析\2_摩根-好博数据分析\好博体育\赛事数据"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    excel_filename = "赛事数据.xlsx"
    excel_path = os.path.join(output_dir, excel_filename)

    if not combined_data.empty:
        print(f"正在生成合并的 Excel 文件：{excel_path}")
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            combined_data.to_excel(writer, sheet_name="赛事数据", index=False)
            ws = writer.sheets["赛事数据"]
            ws.freeze_panes = 'A2'
            ws.auto_filter.ref = ws.dimensions
        print(f"合并的 Excel 文件生成完成：{excel_path}")
    else:
        print("没有数据可写入，未生成 Excel 文件。")

    print("所有目标场馆数据处理完成。")
    end_time = datetime.now()
    print(f"运行结束时间 {end_time.strftime('%Y-%m-%d %H:%M')}")

