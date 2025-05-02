import os
import asyncio
from telegram import Bot
from telegram.error import TelegramError
import pymysql
import pandas as pd
from datetime import datetime

# 配置
FOLDER_PATH = r'C:/Henvita/1_定时注单导出/收费站/'
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'offline_bigdata'
}
TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
CHAT_ID = '7523061850'

QUERY_PARAMS = {
    'start_time': '2025-04-01 00:00:00',
    'end_time': '2025-04-30 23:59:59',
    'site_id': '1000',
    'top_list': ['agtest2', 'agtest1', 'ceshidl1', 'ceshidl2', 'ceshimark1', 'luv1230']
}
OUTPUT_FILENAME = "代理报表.txt"
OUTPUT_COLUMNS = ['网站ID', '代理名称', '统计日期', '注册人数', '首存人数', '存款人数', '存款额', '投注人数（结算）', '有效投注额（结算）', '公司输赢（结算）', '红利', '返水']
COLUMN_MAPPING = {
    '注册会员数量': '注册人数',
    '首次充值会员数量': '首存人数',
    '充值会员数量': '存款人数',
    '充值金额': '存款额',
    '投注会员数量结算': '投注人数（结算）',
    '有效投注金额结算': '有效投注额（结算）',
    '公司盈亏结算': '公司输赢（结算）',
    '红利金额': '红利',
    '返利金额': '返水'
}
TASK_INTERVAL = 1800  # 每30分钟
MAX_RETRIES = 3
RETRY_DELAY = 10

async def send_telegram_message(bot, chat_id, message):
    try:
        await bot.send_message(chat_id=chat_id, text=message)
    except TelegramError as e:
        print(f"发送消息出错: {e}")

def export_data():
    try:
        with pymysql.connect(**DB_CONFIG) as connection:
            query = f"""SELECT * FROM offline_bigdata.top_daily_report
                        WHERE site_id = '{QUERY_PARAMS['site_id']}'
                        AND statics_date BETWEEN '{QUERY_PARAMS['start_time']}' AND '{QUERY_PARAMS['end_time']}'
                        AND agent_name IN {tuple(QUERY_PARAMS['top_list'])}
                        ORDER BY statics_date, top_id;"""
            df = pd.read_sql_query(query, connection)
    except pymysql.Error as e:
        print(f"数据库错误: {e}")
        return None

    if df.empty:
        print("未查询到数据。")
        return None

    df.columns = ['id', '网站ID', '邀请代码', '代理名称', '统计日期', '顶级ID',
                  '系统类型', '注册会员数量', '首次充值会员数量', '首次充值金额', '首次有效充值会员数量',
                  '首次有效充值金额', '充值会员数量', '提款会员数量', '充值金额', '提款金额',
                  '代理提款金额', '存款调整增加金额', '存款调整减少金额', '投注会员数量', '投注金额',
                  '有效投注金额', '净金额', '红利金额', '代理红利金额', '返利金额', '每个佣金金额',
                  '团队佣金金额', '赢亏调整金额', '系统调整金额', '提前结算净金额', '存款调整金额',
                  '场馆金额', '其他调整金额', '评分调整', '首次充值注册比例', '每个首次充值',
                  '净投注金额比例', '公司盈亏', '公司盈亏结算', '净投注金额比例结算', '投注会员数量结算',
                  '投注金额结算', '有效投注金额结算', '净金额结算', '提前结算净金额结算', '场馆金额结算',
                  '修复净利润', '推广红利', '存款费用', '提现费用', '更新时间']

    df = df[list(COLUMN_MAPPING.keys()) + ['网站ID', '代理名称', '统计日期']]
    df = df.rename(columns=COLUMN_MAPPING)
    df['代理名称'] = df['代理名称'].str.replace('hbvip88', '___hbvip88', regex=False)
    df = df[OUTPUT_COLUMNS]

    output_path = os.path.join(FOLDER_PATH, OUTPUT_FILENAME)
    try:
        df.to_csv(output_path, sep='\t', index=False, encoding='utf-8')
        print(f"数据已导出到: {output_path}")
        return output_path
    except Exception as e:
        print(f"保存文件出错: {e}")
        return None

async def send_file_with_retry(bot, chat_id, file_path, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        return False

    for attempt in range(max_retries):
        try:
            with open(file_path, 'rb') as file:
                await bot.send_document(chat_id=chat_id, document=file)
            print(f"文件已成功发送: {os.path.basename(file_path)}")
            os.remove(file_path)
            print(f"已删除文件: {file_path}")
            return True
        except TelegramError as e:
            print(f"发送文件 {os.path.basename(file_path)} 失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                print(f"达到最大重试次数，文件发送失败。")
                return False
        except Exception as e:
            print(f"发送或删除文件时发生其他错误: {e}")
            return False
    return False

async def main():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    while True:
        print(f"开始执行任务: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        exported_file = export_data()
        if exported_file:
            await send_file_with_retry(bot, CHAT_ID, exported_file)
        else:
            await send_telegram_message(bot, CHAT_ID, "本次定时任务没有生成报表。")
        print(f"任务执行完毕，等待下一次执行。")
        await asyncio.sleep(TASK_INTERVAL)

def run_bot():
    asyncio.run(main())

if __name__ == "__main__":
    run_bot()