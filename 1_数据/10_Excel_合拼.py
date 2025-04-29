import pandas as pd
import os

def merge_excel_files(folder_path, output_filename="合并后的数据.xlsx"):
    """
    合并指定文件夹下的所有 Excel 文件，并将结果保存到一个新的 Excel 文件中。

    Args:
        folder_path (str): 包含要合并的 Excel 文件的文件夹路径。
        output_filename (str, optional): 输出 Excel 文件的名称。默认为 "合并后的数据.xlsx"。
    """
    all_data = []
    first_file_header = None

    excel_files = [f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls'))]

    if not excel_files:
        print(f"在路径 '{folder_path}' 下没有找到 Excel 文件。")
        return

    for i, file in enumerate(excel_files):
        file_path = os.path.join(folder_path, file)
        try:
            df = pd.read_excel(file_path)
            if i == 0:
                first_file_header = list(df.columns)
                all_data.append(df)
            else:
                current_header = list(df.columns)
                if current_header == first_file_header:
                    all_data.append(df)
                else:
                    print(f"警告: 文件 '{file}' 的列名与第一个文件不一致，已跳过。")
        except Exception as e:
            print(f"读取文件 '{file}' 时发生错误: {e}")

    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        output_path = os.path.join(folder_path, output_filename)
        try:
            writer = pd.ExcelWriter(output_path, engine='openpyxl')
            merged_df.to_excel(writer, index=False, freeze_panes=(1, 0))
            writer.close()
            print(f"成功合并 {len(excel_files)} 个 Excel 文件，结果已保存到 '{output_path}'。")
        except Exception as e:
            print(f"保存合并后的数据时发生错误: {e}")
    else:
        print("没有可合并的数据。")

# 请将您的文件夹路径替换为下面的路径
folder_path = r"C:\Henvita\0_数据导出"
merge_excel_files(folder_path)