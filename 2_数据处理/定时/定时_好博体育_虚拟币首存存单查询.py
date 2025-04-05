import os
import asyncio
from telegram import Bot
from telegram.error import TelegramError
import pymysql
import pandas as pd
import datetime
import time

folder_path = 'C:/Henvita/1_定时注单导出/收费站/'
TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
CHAT_ID = '7523061850'
bot = Bot(token=TELEGRAM_BOT_TOKEN)


def export_data_from_database_xunibicunkuan(): # 导出虚拟币存款数据
    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='finance_1000'
    )

    query = """
    WITH ranked_records AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY updated_at) AS rn
        FROM finance_1000.finance_pay_records
        WHERE pay_status IN (2, 3)
          AND pay_type IN (1003, 1018)
    )
    SELECT
        site_id, member_id, member_username, member_grade, top_id, bill_no, typay_order_id, order_amount, pay_amount, 
        pay_seq, rebate_amount, score_amount, category, pay_type, flow_ratio, pay_status, pay_result, confirm_at, operator, complete_time
    FROM ranked_records
    WHERE rn = 1;
    """

    df = pd.read_sql_query(query, connection)
    connection.close()
    df.columns = ['站点ID', '会员ID', '会员用户名', '会员等级', '上级ID', '账单号', '通用支付订单号', '订单金额',
                  '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例', '支付状态',
                  '支付结果', '确认时间', '操作员', '完成时间']
    df['支付方式'] = df['支付方式'].map({1003: '虚拟币扫码', 1018: 'EBPAY'})

    today = datetime.datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    df['确认时间'] = pd.to_datetime(df['确认时间'])
    df_yesterday = df[(df['确认时间'] >= yesterday) & (df['确认时间'] < today)]
    df_1000 = df_yesterday[df_yesterday['站点ID'] == 1000]
    df_1000.to_excel(folder_path + "好博体育 " + yesterday + " 虚拟币首存单号.xlsx", index=False)


def delete_all_files_in_directory(directory):
    if os.path.exists(directory) and os.path.isdir(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"删除文件: {file_path}")
    else:
        print("指定的目录不存在或不是一个目录。")


async def send_files_in_folder(bot, folder_path, chat_id):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'rb') as file:
                    await bot.send_document(chat_id=chat_id, document=file)
                print(f"文件已发送: {file_name}")
            except TelegramError as e:
                print(f"发送文件 {file_name} 时出错: {e}")


async def main():
    while True:
        await asyncio.sleep(get_time())
        export_data_from_database_xunibicunkuan()
        await send_files_in_folder(bot, folder_path, CHAT_ID)
        delete_all_files_in_directory(folder_path)


def get_time():
    now = datetime.datetime.now()
    next_send_time = now.replace(hour=12, minute=00, second=0, microsecond=0)
    if now >= next_send_time:
        next_send_time += datetime.timedelta(days=1)
    seconds_until_next_send = (next_send_time - now).total_seconds()
    print(f"下一次发送将在 {next_send_time.strftime('%Y-%m-%d %H:%M:%S')} 发生。")
    return seconds_until_next_send


def run_bot():
    asyncio.run(main())


if __name__ == "__main__":
    run_bot()