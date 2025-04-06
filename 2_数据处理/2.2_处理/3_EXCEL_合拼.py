import pandas as pd
import os

def merge_and_split_excel(directory, output_prefix="merged_part_", rows_per_file=1000000):
    all_data = []
    for filename in os.listdir(directory):
        if filename.endswith(".xlsx"):
            filepath = os.path.join(directory, filename)
            try:
                df = pd.read_excel(filepath)
                all_data.append(df)
                print(f"成功读取并追加文件: {filename}")
            except Exception as e:
                print(f"读取文件 {filename} 时发生错误: {e}")

    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        num_rows = len(merged_df)
        num_parts = (num_rows // rows_per_file) + (1 if num_rows % rows_per_file > 0 else 0)

        for i in range(num_parts):
            start_row = i * rows_per_file
            end_row = min((i + 1) * rows_per_file, num_rows)
            df_part = merged_df.iloc[start_row:end_row]
            output_filename = f"{output_prefix}{i+1}.xlsx"
            output_filepath = os.path.join(directory, output_filename)
            try:
                df_part.to_excel(output_filepath, index=False)
                print(f"成功保存文件: {output_filename} (行 {start_row} 到 {end_row})")
            except Exception as e:
                print(f"保存文件 {output_filename} 时发生错误: {e}")
        print(f"所有 Excel 文件已成功合并并分割保存到目录: {directory}，共 {num_parts} 个文件。")
    else:
        print(f"在目录 {directory} 中没有找到任何 .xlsx 文件。")

if __name__ == "__main__":
    target_directory = r"C:\Users\Administrator\Downloads\Telegram Desktop"  # 修改回你的原始目录
    merge_and_split_excel(target_directory)