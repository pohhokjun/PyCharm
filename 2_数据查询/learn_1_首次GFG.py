import pymongo
import pandas as pd
import os
from datetime import datetime, timedelta

# print("设置为localhost需要放在服务器本地上跑，设置为18.178.159.230可以在台式电脑上面跑")

# ============ 1. 基础配置 ============
# 连接 MongoDB 数据库
client = pymongo.MongoClient("mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/")  # 在台式电脑上面跑
# client = pymongo.MongoClient("mongodb://biddata:uvb5SOSmLH8sCoSU@localhost:27217/")  # 在服务器上面跑
db = client["update_records"]  # 切换到目标数据库

# 获取用户输入的开始日期和结束日期
start_date = input("请输入开始日期 (格式: YYYY-MM-DD): ")
end_date = input("请输入结束日期 (格式: YYYY-MM-DD): ")
venue = input("请输入场馆名称 (例如: GFDZ): ")  # 例如 "北京体育馆"
# site_num = input("请输入站点ID编号: ")

# 设置筛选条件
flag_value = 1
batch_size = 100000  # 每次读取的最大数据量
# download_folder = "/home/om/duncan/data"
download_folder = r"C:\Users\USER\Desktop\注单导出"

# ============ 2. 准备输出文件夹 ============
os.makedirs(download_folder, exist_ok=True)
for file_name in os.listdir(download_folder):
    file_path = os.path.join(download_folder, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)

# 仅获取集合名称以 "pull_order_game_" 开头并以 "GFDZ" 结尾的集合（示例）
collections = [
    col for col in db.list_collection_names()
    if col.startswith("pull_order_game_") and col.endswith(venue)
]

# ============ 3. 遍历符合条件的集合 ============
for collection_name in collections:
    collection = db[collection_name]

    # 用后4位字符作为文件名的一部分
    collection_suffix = collection_name[-4:]
    file_name = f"{start_date}_{end_date}_{collection_suffix}_注单数据.txt"
    file_path = os.path.join(download_folder, file_name)

    print(f"\n正在处理集合: {collection_name} -> 输出文件: {file_path}")

    # 用于记录某会员是否已经出现过
    member_seen = set()

    # ============ 3.1 获取按 bet_time 升序的游标 ============
    # 仅匹配 flag=1，batch_size 控制单次读取数量
    cursor = collection.find({"flag": flag_value}).sort("bet_time", pymongo.ASCENDING).batch_size(batch_size)

    total_written = 0   # 总输出行数
    header_written = False

    # 以追加模式打开文件（若文件不存在会自动创建）
    with open(file_path, "a", encoding="utf-8") as outfile:
        for doc in cursor:
            # (1) 判断“是否首次投注”
            member_id = doc.get("member_id")
            if member_id not in member_seen:
                is_first_bet = "是"
                member_seen.add(member_id)
            else:
                is_first_bet = "否"

            # (2) 判断 settle_time 是否在 [start_date, end_date] 范围内
            settle_time = doc.get("settle_time", "")
            # 仅比较前 10 位 (YYYY-MM-DD)
            settle_date_str = settle_time[:10]
            if settle_date_str >= start_date and settle_date_str <= end_date:
                # (3) 构建输出行
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
                    "是否首次投注": is_first_bet,  # 最早出现则“是”，否则“否”
                }

                # (4) 写出到文件
                df = pd.DataFrame([row])
                df.to_csv(outfile, sep="\t", index=False, header=not header_written)
                header_written = True
                total_written += 1

                # (5) 若处理数量很大，可在此加日志
                if total_written % batch_size == 0:
                    print(f"已输出 {total_written} 行 -> {file_name}")

    print(f"集合 {collection_name} 处理完成，共输出 {total_written} 行数据 -> {file_name}")


