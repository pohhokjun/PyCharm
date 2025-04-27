import os
import schedule
import time
from datetime import datetime, timedelta
import pymysql
import pandas as pd
import numpy as np
import shutil
from openpyxl import load_workbook
import xlwings as xw
from pathlib import Path
import asyncio
from telegram import Bot
from telegram.error import TelegramError

# --- 配置 ---
TEMPLATE_PATH = "C:/Henvita/模版.xlsx"
OUTPUT_FOLDER = 'C:/Henvita/1_定时注单导出/收费站/'
DB_HOST = '18.178.159.230'
DB_PORT = 3366
DB_USER = 'bigdata'
DB_PASSWORD = 'uvb5SOSmLH8sCoSU'
DB_NAME = 'finance_1000'
TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
CHAT_ID = '7523061850'
SITE_ID_MAPPING = {
    1000: "好博体育",
    2000: "黄金体育",
    4000: "HOME体育",
    5000: "亚洲之星",
    6000: "玖博体育",
    7000: "蓝火体育",
    8000: "A7体育",
}
GROUP_PROFIT_RATE = 0.12
NEGATIVE_COLUMNS = ['红利', '返水', '集团分成', '代理佣金']
SUM_COLUMNS = ['注册数', '首存人数', '首存额', '存款人数', '取款人数', '存款额', '取款额', '存提差',
               '投注人数', '投注额', '有效投注额', '公司输赢', '提前结算', '账户调整', '红利', '返水',
               '代理佣金', '打赏收入', '集团分成', '公司净收入']
CONVERSION_RATE_COLUMNS = {'首存人数': '注册数', '取款额': '存款额',
                           '公司输赢含提前结算(结算)': '投注额(结算)'}
AVERAGE_COLUMNS = {'首存额': '首存人数'}
PROFIT_COLUMNS = {'公司输赢含提前结算(结算)': '投注额(结算)'}


# --- 工具函数 ---
def format_percentage(numerator, denominator):
    return f'{(numerator / denominator):.2%}' if denominator else '0.00%'


def format_float(numerator, denominator):
    return round(numerator / denominator, 2) if denominator else 0


def delete_all_files_in_directory(directory):
    if os.path.exists(directory) and os.path.isdir(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"删除文件: {file_path}")
    else:
        print("指定的目录不存在或不是一个目录。")


def calculate_totals(df):
    if df.empty:
        return df

    total_row = pd.DataFrame(df[SUM_COLUMNS].sum()).T
    for num_col, den_col in CONVERSION_RATE_COLUMNS.items():
        if num_col in total_row.columns and den_col in total_row.columns:
            if num_col == '首存人数' and den_col == '注册数':
                total_row['转化率'] = total_row.apply(
                    lambda row: format_percentage(row[num_col], row[den_col]), axis=1)
            elif num_col == '取款额' and den_col == '存款额':
                total_row['提存率'] = total_row.apply(
                    lambda row: format_percentage(row[num_col], row[den_col]), axis=1)
    for num_col, den_col in AVERAGE_COLUMNS.items():
        if num_col in total_row.columns and den_col in total_row.columns:
            total_row['人均首存'] = total_row.apply(
                lambda row: format_float(row[num_col], row[den_col]), axis=1)
    for num_col, den_col in PROFIT_COLUMNS.items():
        if num_col in total_row.columns and den_col in total_row.columns:
            total_row['盈余比例'] = total_row.apply(
                lambda row: format_percentage(row[num_col], row[den_col]), axis=1)

    total_row['集团分成比例'] = f'{GROUP_PROFIT_RATE:.0%}'
    total_row['日期'] = '总计'

    expected_columns = ['日期', '注册数', '首存人数', '转化率', '首存额', '人均首存', '存款人数', '取款人数',
                        '存款额', '取款额', '存提差', '提存率', '投注人数', '投注额', '有效投注额', '公司输赢',
                        '提前结算', '盈余比例', '账户调整', '红利', '返水', '代理佣金', '打赏收入',
                        '集团分成比例', '集团分成', '公司净收入']
    return pd.concat([df, total_row])[expected_columns]


def process_dataframe(df, output_filename):
    if df.empty:
        print(f"DataFrame为空，跳过文件: {output_filename}")
        return

    print(f"写入前的列名: {df.columns.tolist()}")
    print(f"列数: {len(df.columns)}")

    expected_columns = 27
    if len(df.columns) > expected_columns:
        df = df.iloc[:, :expected_columns]
        print(f"裁剪后列名: {df.columns.tolist()}")

    output_path = Path(OUTPUT_FOLDER) / output_filename.replace("（", "(").replace("）", ")")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = output_path.with_name(f"temp_{output_path.name}")
    app = None

    try:
        shutil.copy(TEMPLATE_PATH, temp_output_path)
        app = xw.App(visible=False)
        wb = app.books.open(str(temp_output_path))
        ws = wb.sheets['SheetJS']

        ws.range('A2').options(index=False, header=False).value = df
        ws.range(f'AB2:ZZ{ws.used_range.last_cell.row}').clear()

        # 设置百分比列格式为两位小数
        percentage_columns = {'转化率': 'D', '提存率': 'L', '盈余比例': 'R', '集团分成比例': 'X'}
        last_row = ws.used_range.last_cell.row
        for col_name, col_letter in percentage_columns.items():
            ws.range(f'{col_letter}2:{col_letter}{last_row}').number_format = '0.00%'

        total_row_index = None
        for i, row in enumerate(ws.range(f'A1:A{ws.used_range.last_cell.row}').value):
            if row == '总计':
                total_row_index = i + 1
                break

        if total_row_index and (total_row_index + 1) < 34:
            delete_rows = 34 - (total_row_index + 1)
            ws.range(f"{total_row_index + 1}:{total_row_index + delete_rows}").delete(shift='up')

        if output_path.exists():
            output_path.unlink()
        wb.save(str(output_path))
        wb.close()

    except Exception as e:
        print(f"处理文件 {output_filename} 出错: {e}")
    finally:
        if app:
            app.quit()
        if temp_output_path.exists():
            try:
                temp_output_path.unlink()
            except Exception as e:
                print(f"删除临时文件 {temp_output_path} 失败: {e}")


async def send_files(bot, folder_path, chat_id):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'rb') as file:
                    await bot.send_document(chat_id=chat_id, document=file)
                print(f"已发送: {file_name}")
            except TelegramError as e:
                print(f"发送 {file_name} 失败: {e}")


def get_seconds_until(target_hour, target_minute):
    now = datetime.now()
    next_target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if now >= next_target:
        next_target += timedelta(days=1)
    seconds = (next_target - now).total_seconds()
    print(f"下次执行: {next_target.strftime('%Y-%m-%d %H:%M:%S')}")
    return seconds


async def scheduled_task():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    while True:
        wait_seconds = get_seconds_until(13, 0)
        await asyncio.sleep(wait_seconds)
        await main_job()
        await send_files(bot, OUTPUT_FOLDER, CHAT_ID)
        delete_all_files_in_directory(OUTPUT_FOLDER)


async def main_job():
    print("当前时间:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    yesterday = datetime.now() - timedelta(days=1)
    current_month = yesterday.strftime('%Y-%m')
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    try:
        with pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
                             database=DB_NAME) as connection:
            query = "SELECT * FROM bigdata.platform_daily_report;"
            df = pd.read_sql_query(query, connection)
    except pymysql.Error as e:
        print(f"数据库连接或查询失败: {e}")
        return

    df.columns = ['id', '日期', '站点ID', '渠道类型', '存款调整上分金额', '存款调整下分金额', '存款额', '取款额',
                  '注册数',
                  '首存人数', '首存额', '首次有效充值会员数', '首次有效充值金额', '存款人数', '取款人数', '投注人数',
                  '有效投注', '投注额', '公司输赢含提前结算', '红利', '返水', '个人佣金金额', '团队佣金金额',
                  '提前结算净额', '账户调整', '首次充值注册比例', '人均首次充值', '存提差', '提款充值比例',
                  '净投注金额比例', '公司输赢', '集团分成', '团队金额比例', '场馆费', '投注人数(结算)',
                  '有效投注(结算)', '投注额(结算)', '公司输赢含提前结算(结算)', '提前结算(结算)', '公司输赢(结算)',
                  '盈余比例(结算)', '场馆费(结算)', '打赏收入', '集团分成(结算)', '存款手续费', '提款手续费',
                  '分成比例']

    df['渠道类型'] = df['渠道类型'].replace({-1: '直客', 0: '普通代理', 1: '官方代理'})

    grouped_data = df.groupby(['站点ID', '日期']).agg({
        '注册数': 'sum', '首存人数': 'sum', '首存额': 'sum', '存款人数': 'sum', '存款额': 'sum',
        '取款人数': 'sum', '取款额': 'sum', '存提差': 'sum', '账户调整': 'sum', '投注人数(结算)': 'sum',
        '投注额(结算)': 'sum', '有效投注(结算)': 'sum', '公司输赢含提前结算(结算)': 'sum', '提前结算(结算)': 'sum',
        '红利': 'sum', '返水': 'sum', '个人佣金金额': 'sum', '团队佣金金额': 'sum', '打赏收入': 'sum'
    }).reset_index()

    grouped_data['转化率'] = grouped_data.apply(
        lambda row: format_percentage(row['首存人数'], row['注册数']), axis=1)
    grouped_data['人均首存'] = grouped_data.apply(
        lambda row: format_float(row['首存额'], row['首存人数']), axis=1)
    grouped_data['提存率'] = grouped_data.apply(
        lambda row: format_percentage(row['取款额'], row['存款额']), axis=1)
    grouped_data['盈余比例'] = grouped_data.apply(
        lambda row: format_percentage(row['公司输赢含提前结算(结算)'], row['投注额(结算)']), axis=1)

    grouped_data['公司输赢'] = grouped_data['公司输赢含提前结算(结算)'] - grouped_data['提前结算(结算)']
    grouped_data['集团分成比例'] = f'{GROUP_PROFIT_RATE:.0%}'
    grouped_data['集团分成(结算)'] = (grouped_data['公司输赢含提前结算(结算)'] + grouped_data[
        '账户调整']) * GROUP_PROFIT_RATE
    grouped_data['代理佣金'] = grouped_data['个人佣金金额'] + grouped_data['团队佣金金额']
    grouped_data['公司净收入'] = grouped_data['公司输赢含提前结算(结算)'] + grouped_data['账户调整'] - grouped_data[
        '红利'] - grouped_data['返水'] - grouped_data['代理佣金'] - grouped_data['集团分成(结算)']
    grouped_data = grouped_data.drop(columns=['个人佣金金额', '团队佣金金额'])
    grouped_data.columns = ['站点ID', '日期', '注册数', '首存人数', '首存额', '存款人数', '存款额', '取款人数',
                            '取款额', '存提差',
                            '账户调整', '投注人数', '投注额', '有效投注额', '公司输赢含提前结算', '提前结算',
                            '红利', '返水', '打赏收入', '转化率', '人均首存', '提存率', '盈余比例', '公司输赢',
                            '集团分成比例', '集团分成',
                            '代理佣金', '公司净收入']
    grouped_data = grouped_data[
        ['站点ID', '日期', '注册数', '首存人数', '转化率', '首存额', '人均首存', '存款人数', '取款人数',
         '存款额', '取款额', '存提差', '提存率', '投注人数', '投注额', '有效投注额', '公司输赢', '提前结算',
         '盈余比例', '账户调整', '红利', '返水', '代理佣金', '打赏收入', '集团分成比例', '集团分成', '公司净收入']]

    for col in NEGATIVE_COLUMNS:
        if col in grouped_data.columns:
            grouped_data[col] = -grouped_data[col]

    monthly_data = grouped_data[grouped_data['日期'].str.startswith(current_month)].copy()
    monthly_data['日期'] = pd.to_datetime(monthly_data['日期'])
    monthly_data = monthly_data[monthly_data['日期'] <= pd.Timestamp(yesterday)].copy()
    monthly_data['日期'] = monthly_data['日期'].dt.strftime('%Y-%m-%d')

    file_date_str = yesterday.strftime('%Y.%m.%d')
    dataframes = {
        f"{name}平台日报表({file_date_str}).xlsx": monthly_data[monthly_data['站点ID'] == site_id].drop(
            columns=['站点ID'])
        for site_id, name in SITE_ID_MAPPING.items()
    }

    for filename, df in dataframes.items():
        processed_df = calculate_totals(df)
        process_dataframe(processed_df, filename)


async def main():
    task = asyncio.create_task(scheduled_task())
    try:
        await task
    except asyncio.CancelledError:
        print("任务被取消")
    except Exception as e:
        print(f"主程序出错: {e}")


def run():
    asyncio.run(main())


if __name__ == '__main__':
    run()
