import os
import re
import pandas as pd

# 目录路径
directory = r"C:\Henvita\1_数据导出"
output_file = r"C:\Henvita\练习.xlsx"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# 正则匹配 3.14 后面到 "注单数据" 之间的关键词
pattern = re.compile(r"3\.14(.*?)注单数据")

# 存储分类后的数据
game_data = {}

# 遍历文件夹
for file in os.listdir(directory):
    if file.endswith(".xlsx") and not file.startswith("~$"):  # 忽略 Excel 临时文件
        match = pattern.search(file)
        if match:
            game_type = match.group(1).strip()  # 提取游戏类型
            file_path = os.path.join(directory, file)

            # 读取 Excel 文件
            try:
                df = pd.read_excel(file_path)

                # 删除“是否提前结算”字段
                if "是否提前结算" in df.columns:
                    df.drop(columns=["是否提前结算"], inplace=True)

                # 添加“今天是第几笔投资”字段
                if {"会员账号", "投注时间"}.issubset(df.columns):
                    df["投注时间"] = pd.to_datetime(df["投注时间"])

                    # **按照会员账号 & 日期进行排序**
                    df.sort_values(by=["会员账号", "投注时间"], ascending=[True, True], inplace=True)

                    # **计算当天第几笔投资**
                    df["今天是第几笔投资"] = df.groupby(["会员账号", df["投注时间"].dt.date])["投注时间"].rank(method="first").astype(int)

                if game_type not in game_data:
                    game_data[game_type] = []
                game_data[game_type].append(df)
            except Exception as e:
                print(f"读取文件 {file} 失败: {e}")

# 写入到一个 Excel 文件，不同游戏类型存入不同 Sheet
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    for game_type, dfs in game_data.items():
        final_df = pd.concat(dfs, ignore_index=True)  # 合并同类数据

        # **处理超过 Excel Sheet 行数限制的情况**
        max_rows = 1_048_576  # Excel 限制
        sheet_count = 1  # Sheet 计数

        for start_row in range(0, len(final_df), max_rows):
            sheet_name = f"{game_type[:25]}_{sheet_count}"  # 避免 sheet 名称过长
            sub_df = final_df.iloc[start_row:start_row + max_rows]

            # **写入 Excel 并冻结首行 + 添加筛选**
            sub_df.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            worksheet.freeze_panes(1, 0)  # 冻结首行
            worksheet.autofilter(0, 0, 0, len(sub_df.columns) - 1)  # **第一行添加筛选**

            sheet_count += 1  # 更新 Sheet 计数

print(f"合并完成：{output_file}")
