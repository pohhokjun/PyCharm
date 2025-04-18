import pymongo
import pandas as pd
import os
from datetime import datetime
import openpyxl
from openpyxl.utils import get_column_letter

# ============ 1. 基础配置 ============
# 记录开始时间
start_time = datetime.now()
print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")

# 连接 MongoDB 数据库
client = pymongo.MongoClient("mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/")
db = client["update_records"]

# 固定参数
start_date = "2025-03-31"
end_date = "2025-03-31"
venue = "DBDJ"

# 设置筛选条件
flag_value = 1
batch_size = 100000

# ============ 2. 准备输出文件夹 ============
# 使用脚本所在目录
download_folder = os.path.dirname(os.path.abspath(__file__))
os.makedirs(download_folder, exist_ok=True)
for file_name in os.listdir(download_folder):
    file_path = os.path.join(download_folder, file_name)
    if os.path.isfile(file_path) and file_name.endswith('.xlsx'):
        os.remove(file_path)

# 获取集合
collections = [
    col for col in db.list_collection_names()
    if col.startswith("pull_order_game_") and col.endswith(venue)
]

# ============ 3. 遍历集合 ============
for collection_name in collections:
    collection = db[collection_name]

    # 文件名：脚本名_时间_日期_时间
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    current_time = datetime.now().strftime("%Y-%m-%d_%H.%M")
    collection_suffix = collection_name[-4:]
    file_name = f"{script_name}_{current_time}_{start_date}_{end_date}_{collection_suffix}.xlsx"
    file_path = os.path.join(download_folder, file_name)

    print(f"\n正在处理集合: {collection_name} -> 输出文件: {file_path}")

    # 获取游标
    cursor = collection.find({"flag": flag_value}).sort("bet_time", pymongo.ASCENDING).batch_size(batch_size)

    total_written = 0
    data_rows = []

    for doc in cursor:
        # 判断 settle_time 是否在日期范围内
        settle_time = doc.get("settle_time", "")
        settle_date_str = settle_time[:10]
        if settle_date_str >= start_date and settle_date_str <= end_date:
            # 构建输出行
            row = {
                "站点ID": doc.get("site_id"),
                "结算日期": settle_date_str,
                "会员编号": doc.get("member_id"),
                "会员账号": doc.get("member_name"),
                "场馆名称": doc.get("venue_name"),
                "游戏名称": doc.get("game_name"),
                "赔率类型": doc.get("odds_type"),
                "赔率": doc.get("odds"),
                "投注额": doc.get("bet_amount"),
                "有效投注": doc.get("valid_bet_amount"),
                "会员输赢": doc.get("net_amount"),
                "是否提前结算": doc.get("early_settle_flag"),
                "投注时间": doc.get("bet_time"),
                "开始时间": doc.get("start_time"),
                "结算时间": doc.get("settle_time"),
                "注单号": doc.get("id"),
                "游戏详情": doc.get("play_info"),
                "赛事ID": doc.get("match_id"),
            }
            data_rows.append(row)
            total_written += 1

            # 批量写入
            if total_written % batch_size == 0:
                print(f"已处理 {total_written} 行 -> {file_name}")

    # 写入 Excel
    if data_rows:
        df = pd.DataFrame(data_rows)
        df.to_excel(file_path, index=False, engine="openpyxl")

        # 设置冻结首行和筛选
        wb = openpyxl.load_workbook(file_path)
        ws = wb.active
        ws.freeze_panes = "A2"  # 冻结首行
        ws.auto_filter.ref = ws.dimensions  # 启用筛选
        wb.save(file_path)

    print(f"集合 {collection_name} 处理完成，共输出 {total_written} 行数据 -> {file_name}")

# 记录结束时间
end_time = datetime.now()
print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")