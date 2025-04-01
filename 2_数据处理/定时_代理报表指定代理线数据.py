import os
import asyncio
from telegram import Bot
from telegram.error import TelegramError
import pymysql
import pandas as pd
import datetime
import time

folder_path = 'C:\Henvita\1_定时注单导出\代理定时数据'

def export_data_from_database():
    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='finance_1000'
    )

    # --------------------------------------
    # 获取当前年月，以 "YYYY-MM" 格式表示
    # current_month = datetime.datetime.now().strftime("%Y-%m")
    # current_month = current_month + "-01" + "00:00:00"
    # start_time = '2024-12-31 00:00:00'  # 开始时间
    # --------------------------------------


    # --------------------------------------
    # 获取当前日期
    today2 = datetime.datetime.now()
    # 计算上个月的最后一天
    last_day_of_last_month = today2.replace(day=1) - datetime.timedelta(days=1)
    # 将日期格式化为 "YYYY-MM-DD"
    last_day_str = last_day_of_last_month.strftime("%Y-%m-%d")
    # print(last_day_str)
    start_time = last_day_str + " 00:00:00"  # 开始时间
    # --------------------------------------
    

    today = datetime.datetime.now().strftime('%Y-%m-%d')  # 今天日期
    print(today)

    end_time = today + ' 23:59:59'  # 结束时间

    site_id = '1000'  # 站点ID
    top_list = ['gztg101001', 'gztg102001', 'gztg103001', 'gztg104001', 'hbvip88', 'gztg105001']

    # 使用f-string格式化查询语句
    # query = f"SELECT * FROM bigdata.top_daily_report WHERE site_id = '{site_id}' AND statics_date = '{today}' AND agent_name IN {tuple(top_list)}"

    # query = f"""
    # SELECT *
    # FROM bigdata.top_daily_report
    # WHERE site_id = '{site_id}'
    # AND statics_date BETWEEN '{start_time}' AND '{end_time}'
    # AND agent_name IN {tuple(top_list)}
    # """

    query = f"""SELECT *
            FROM bigdata.top_daily_report
            WHERE site_id = '{site_id}'
              AND statics_date BETWEEN '{start_time}' AND '{end_time}' 
              AND agent_name IN {tuple(top_list)}
            ORDER BY statics_date, top_id;"""

    df = pd.read_sql_query(query, connection)
    print(df.columns)
    # ['id', 'site_id', 'invite_code', 'agent_name', 'statics_date', 'top_id',
    #        'sys_type', 'register_member_count', 'first_recharge_member_count',
    #        'first_recharge_amount', 'first_valid_recharge_member_count',
    #        'first_valid_recharge_amount', 'recharge_member_count',
    #        'drawing_member_count', 'recharge_amount', 'drawing_amount',
    #        'agent_drawing_amount', 'deposit_adjust_more_amount',
    #        'deposit_adjust_less_amount', 'bet_member_count', 'bet_amount',
    #        'valid_bet_amount', 'net_amount', 'dividend_amount',
    #        'agent_dividend_amount', 'rebate_amount', 'per_commission_amount',
    #        'team_commission_amount', 'win_loss_adjust_amount',
    #        'system_adjust_amount', 'early_settle_net_amount',
    #        'deposit_adjust_amount', 'venue_amount', 'other_adjust_amount',
    #        'score_adjust', 'first_recharge_register_ratio', 'per_first_recharge',
    #        'net_bet_amount_ratio', 'company_win_lose', 'company_win_lose_settle',
    #        'net_bet_amount_ratio_settle', 'bet_member_count_settle',
    #        'bet_amount_settle', 'valid_bet_amount_settle', 'net_amount_settle',
    #        'early_settle_net_amount_settle', 'venue_amount_settle',
    #        'repair_net_profit', 'promotion_dividend', 'deposit_fee',
    #        'withdraw_fee', 'update_at']
    print(df)

    connection.close()

    df.columns = ['id', '网站ID', '邀请代码', '代理名称', '统计日期', '顶级ID',
                  '系统类型', '注册会员数量', '首次充值会员数量',
                  '首次充值金额', '首次有效充值会员数量',
                  '首次有效充值金额', '充值会员数量',
                  '提款会员数量', '充值金额', '提款金额',
                  '代理提款金额', '存款调整增加金额',
                  '存款调整减少金额', '投注会员数量', '投注金额',
                  '有效投注金额', '净金额', '红利金额',
                  '代理红利金额', '返利金额', '每个佣金金额',
                  '团队佣金金额', '赢亏调整金额',
                  '系统调整金额', '提前结算净金额',
                  '存款调整金额', '场馆金额', '其他调整金额',
                  '评分调整', '首次充值注册比例', '每个首次充值',
                  '净投注金额比例', '公司盈亏', '公司盈亏结算',
                  '净投注金额比例结算', '投注会员数量结算',
                  '投注金额结算', '有效投注金额结算', '净金额结算',
                  '提前结算净金额结算', '场馆金额结算',
                  '修复净利润', '推广红利', '存款费用',
                  '提现费用', '更新时间']

    # df = df[['网站ID', '代理名称', '统计日期', '注册会员数量', '首次充值会员数量', '首次充值金额',
    #          '首次有效充值会员数量', '首次有效充值金额', '充值会员数量']]

    df = df[['网站ID',
             '代理名称',
             '统计日期',
             '注册会员数量',
             '首次充值会员数量',
             '充值会员数量',
             '充值金额',
             '投注会员数量结算',
             '有效投注金额结算',
             '公司盈亏结算',
             '红利金额',
             '返利金额']]

    # 修改列名
    df.rename(columns={
        '注册会员数量': '注册人数',
        '首次充值会员数量': '首存人数',
        '充值会员数量': '存款人数',
        '充值金额' : '存款额',
        '投注会员数量结算': '投注人数（结算）',
        '有效投注金额结算': '有效投注额（结算）',
        '公司盈亏结算': '公司输赢（结算）',
        '红利金额': '红利',
        '返利金额': '返水'
    }, inplace=True)

    def select(data, column, start_time, end_time):
        data = data[(data[column] >= start_time) & (data[column] < end_time)]
        return data

    # df = select(df, '确认时间', '2024-12-01', '2024-12-02')

    # df.to_excel(folder_path + "代理报表.xlsx", index=False)

    df['代理名称'] = df['代理名称'].str.replace('hbvip88', '___hbvip88', regex=False)
    df.to_csv(folder_path + "代理报表.txt", sep='\t', index=False, encoding='utf-8')

def delete_all_files_in_directory(directory):
    # 确保目录存在
    if os.path.exists(directory) and os.path.isdir(directory):
        # 遍历目录中的所有文件
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            # 检查是否是文件并删除
            if os.path.isfile(file_path):
                os.remove(file_path)  # 删除文件
                print(f"删除文件: {file_path}")
    else:
        print("指定的目录不存在或不是一个目录。")


# Telegram bot token and chat_id
TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
CHAT_ID = '-1002300157208'


bot = Bot(token=TELEGRAM_BOT_TOKEN)

async def send_files_in_folder(bot, folder_path, chat_id):
    # 遍历文件夹中的所有文件
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        # 检查是否为文件（忽略子文件夹）
        if os.path.isfile(file_path):
            try:
                # 发送文件到 chat_id
                with open(file_path, 'rb') as file:
                    await bot.send_document(chat_id=chat_id, document=file)
                print(f"文件已发送: {file_name}")
            except TelegramError as e:
                print(f"发送文件 {file_name} 时出错: {e}")

async def main():
    while True:
        export_data_from_database()

        await send_files_in_folder(bot, folder_path, CHAT_ID)

        delete_all_files_in_directory(folder_path)

        # 等待 3600 秒（1 小时）
        await asyncio.sleep(1800)

# 启动异步任务
def run_bot():
    asyncio.run(main())

# 启动异步任务
if __name__ == "__main__":
    run_bot()
