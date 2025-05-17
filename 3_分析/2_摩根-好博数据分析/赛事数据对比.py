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
    1: '星期一',
    2: '星期二',
    3: '星期三',
    4: '星期四',
    5: '星期五',
    6: '星期六',
    7: '星期日'
}


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
        return [{"record": rec, "coll": coll} for rec in db[coll].aggregate(pipe)]

    with ThreadPoolExecutor(max_workers=5) as executor:
        for result in executor.map(process_collection, cols):
            recs.extend(result)

    data = []
    for item in recs:
        rec = item["record"]
        coll = item["coll"]
        game_play_info = rec.get("game_play_info", "")
        play_info = rec.get("play_info", "")
        game_name = rec.get("game_name", "")
        coll_suffix = coll.split('_')[-1]

        lines = game_play_info.split('\n')
        if len(lines) >= 2 and lines[1].strip() != "":
            second_line = lines[1]
            if ' - ' in second_line:
                event_name = second_line.split(' - ')[0]
            else:
                event_name = second_line
        else:
            play_lines = play_info.split('\n')
            if len(play_lines) >= 2:
                league_line = play_lines[1]
                if "联赛名称:" in league_line:
                    event_name = league_line.split("联赛名称:")[1].strip()
                else:
                    event_name = ""
            else:
                event_name = ""

        if coll_suffix in ['XMTY', 'IMDJ']:
            if len(lines) >= 3:
                team_name = lines[2].strip() if lines[2].strip() else ""
            else:
                team_name = ""
        elif coll_suffix == 'DBDJ':
            if len(lines) >= 2 and lines[1].strip() != "":
                if len(lines) >= 3 and ' ' in lines[2]:
                    team_name = lines[2].split(' ')[0].strip()
                else:
                    team_name = lines[2].strip() if len(lines) >= 3 else ""
            else:
                team_name = game_name

        event_date = rec["start_time"][:10]
        game_type = rec.get("game_type", 0)
        venue_type = game_type_map.get(game_type, "未知")
        data.append({
            "赛事日期": event_date,
            "赛事名称": event_name,
            "球队": team_name,
            "场馆类型": venue_type,
            "member_id": rec["member_id"],
            "valid_bet_amount": rec["valid_bet_amount"]
        })

    df = pd.DataFrame(data)
    grouped = df.groupby(['赛事日期', '赛事名称', '球队', '场馆类型'])
    df_result = grouped.agg({
        'valid_bet_amount': 'sum',
        'member_id': ['count', lambda x: x.nunique()]
    }).reset_index()
    df_result.columns = ['赛事日期', '赛事名称', '球队', '场馆类型', '有效投注', '投注次数', '投注人数']
    return df_result


def merge_period(start_date: str, end_date: str) -> pd.DataFrame:
    """拉取指定时间段的数据"""
    return event_data(start_date, end_date)


if __name__ == '__main__':
    today = date.today()
    yesterday = today - timedelta(days=1)

    # 统一按“昨天”来算，本月 = 昨天所在月的 1 日～昨天；上月 = 昨天所在月的上一整月
    this_start = yesterday.replace(day=1).strftime("%Y-%m-%d")
    this_end = yesterday.strftime("%Y-%m-%d")

    last_month_end = (yesterday.replace(day=1) - timedelta(days=1))
    last_month_start = last_month_end.replace(day=1)
    last_start = last_month_start.strftime("%Y-%m-%d")
    last_end = last_month_end.strftime("%Y-%m-%d")

    # 拉取本月和上月
    with ProcessPoolExecutor(max_workers=2) as executor:
        future_this = executor.submit(merge_period, this_start, this_end)
        future_last = executor.submit(merge_period, last_start, last_end)
        df_this = future_this.result()
        df_last = future_last.result()

    # 合并数据
    final = pd.concat([df_last, df_this], ignore_index=True)

    # 添加星期字段
    final['星期'] = pd.to_datetime(final['赛事日期']).dt.weekday + 1  # 0-6 转为 1-7

    # 移动星期字段到赛事日期后面
    cols = final.columns.tolist()
    date_index = cols.index('赛事日期')
    cols.insert(date_index + 1, cols.pop(cols.index('星期')))
    final = final[cols]

    # 确保输出目录存在
    output_dir = r".\好博体育"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 按照场馆类型分组
    venue_groups = final.groupby('场馆类型')

    for venue, venue_data in venue_groups:
        # 按照星期分组
        weekday_groups = venue_data.groupby('星期')

        # 创建 Excel 文件路径
        excel_path = os.path.join(output_dir, f"{venue}赛事数据源.xlsx")

        # 使用 ExcelWriter 创建文件
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for weekday, weekday_data in weekday_groups:
                sheet_name = weekday_map.get(weekday, f"星期{weekday}")

                # 获取最新一天的数据
                latest_date = weekday_data['赛事日期'].max()
                latest_data = weekday_data[weekday_data['赛事日期'] == latest_date]

                # 获取上月数据
                last_month_data = weekday_data[weekday_data['赛事日期'] < this_start]

                # 按赛事名称和球队分组，计算上月平均值
                avg_data = last_month_data.groupby(['赛事名称', '球队']).agg({
                    '有效投注': 'mean',
                    '投注次数': 'mean',
                    '投注人数': 'mean'
                }).reset_index()

                # 保留星期和场馆类型字段
                latest_data = latest_data[
                    ['赛事日期', '星期', '场馆类型', '赛事名称', '球队', '有效投注', '投注次数', '投注人数']]
                avg_data = avg_data[['赛事名称', '球队', '有效投注', '投注次数', '投注人数']].copy()
                avg_data['星期'] = weekday
                avg_data['场馆类型'] = venue

                # 使用 outer 合并最新一天数据和上月平均值
                merged_data = pd.merge(latest_data, avg_data, on=['星期', '场馆类型', '赛事名称', '球队'],
                                       how='outer', suffixes=('_最新', '_平均'))

                # 对有效投注、投注次数、投注人数字段的空值填充为 0
                for col in ['有效投注_最新', '有效投注_平均', '投注次数_最新', '投注次数_平均', '投注人数_最新',
                            '投注人数_平均']:
                    merged_data[col] = merged_data[col].fillna(0)

                # 计算差值
                merged_data['有效投注差值'] = merged_data['有效投注_最新'] - merged_data['有效投注_平均']
                merged_data['投注次数差值'] = merged_data['投注次数_最新'] - merged_data['投注次数_平均']
                merged_data['投注人数差值'] = merged_data['投注人数_最新'] - merged_data['投注人数_平均']

                # 设置“赛事日期”字段为“同比上月{星期几}差值”
                merged_data['赛事日期'] = f"同比上月{sheet_name}差值"
                print(merged_data.columns)

                # 保留所有其他字段，只选择需要的列
                merged_data = merged_data[['赛事日期', '星期', '赛事名称', '球队', '场馆类型',
                                           '有效投注差值', '投注次数差值', '投注人数差值']]

                # 写入最新一天数据
                latest_data_sheet_name = f"{sheet_name}最新"
                latest_data.to_excel(writer, sheet_name=latest_data_sheet_name, index=False)

                # 写入上月平均值数据
                avg_data['赛事日期'] = f"上月{sheet_name}均值"
                avg_data_sheet_name = f"上月{sheet_name}均值"
                avg_data.to_excel(writer, sheet_name=avg_data_sheet_name, index=False)

                # 写入差值数据到新sheet
                diff_sheet_name = f"{sheet_name}差值"
                merged_data.to_excel(writer, sheet_name=diff_sheet_name, index=False)

        print(f"完成：{excel_path}")