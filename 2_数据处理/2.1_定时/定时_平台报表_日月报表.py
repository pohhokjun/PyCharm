import os
import asyncio
import datetime
import pymysql
import pandas as pd
from telegram import Bot
from telegram.error import TelegramError

# 配置常量
FOLDER_PATH = 'C:/Henvita/1_定时注单导出/收费站'
TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
CHAT_ID = '7523061850'
DB_CONFIG = {
    'host': '18.178.159.230',
    'port': 3366,
    'user': 'bigdata',
    'password': 'uvb5SOSmLH8sCoSU',
    'database': 'finance_1000'
}

# 站点ID映射
SITE_MAPPING = {
    3000: "宾利体育", 4000: "HOME体育", 5000: "亚洲之星",
    6000: "玖博体育", 7000: "蓝火体育", 8000: "A7体育",
    2000: "黄金体育", 1000: "好博体育"
}


def to_thousands_separator(number):
    """格式化数字为千位分隔符格式"""
    return f"{number:,.0f}"


def delete_files(directory):
    """删除目录下所有文件"""
    if os.path.isdir(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"删除文件: {file_path}")


def process_dataframe(df, period='daily'):
    """处理数据框架的通用函数"""
    # 重命名列
    columns = ['id', '日期' if period == 'daily' else '月份', '站点ID', '渠道类型'] + [
        '存款调整上分金额', '存款调整下分金额', '充值金额', '提款金额', '注册人数',
        '首存人数', '首存金额', '首次有效充值会员数', '首次有效充值金额',
        '存款人数', '提款人数', '投注人数', '有效投注', '投注额', '公司输赢含提前结算',
        '红利', '返水', '个人佣金金额', '团队佣金金额', '提前结算净额', '账号调整',
        '首次充值注册比例', '人均首次充值', '存提差', '提款充值比例', '净投注金额比例',
        '公司输赢', '集团分成', '团队金额比例', '场馆费', '投注人数(结算)', '有效投注(结算)',
        '投注额(结算)', '公司输赢含提前结算(结算)', '提前结算(结算)', '公司输赢(结算)',
        '盈余比例(结算)', '场馆费(结算)', '打赏金额', '集团分成(结算)', '存款手续费',
        '提款手续费', '分成比例'
    ]
    df.columns = columns

    # 分组聚合
    group_key = '日期' if period == 'daily' else '月份'
    grouped = df.groupby(['站点ID', group_key]).agg({
        '注册人数': 'sum', '首存人数': 'sum', '首存金额': 'sum',
        '存款人数': 'sum', '充值金额': 'sum', '提款人数': 'sum',
        '提款金额': 'sum', '存提差': 'sum', '账号调整': 'sum',
        '投注人数(结算)': 'sum', '投注额(结算)': 'sum', '有效投注(结算)': 'sum',
        '公司输赢含提前结算(结算)': 'sum', '红利': 'sum', '返水': 'sum',
        '个人佣金金额': 'sum', '团队佣金金额': 'sum'
    }).reset_index()

    # 计算衍生字段
    grouped['集团分成(结算)'] = (grouped['公司输赢含提前结算(结算)'] + grouped['账号调整']) * 0.12
    grouped['代理佣金'] = grouped['个人佣金金额'] + grouped['团队佣金金额']
    grouped['公司净收入'] = (grouped['公司输赢含提前结算(结算)'] + grouped['账号调整'] -
                             grouped['红利'] - grouped['返水'] - grouped['代理佣金'] -
                             grouped['集团分成(结算)'])

    # 删除不需要的列
    grouped = grouped.drop(columns=['个人佣金金额', '团队佣金金额'])

    # 筛选当前月份数据
    current_month = datetime.datetime.now().strftime("%Y-%m")
    grouped = grouped[grouped[group_key].str.startswith(current_month)]

    # 格式化数字列
    numeric_cols = ['注册人数', '首存人数', '首存金额', '存款人数', '充值金额',
                    '提款人数', '提款金额', '存提差', '账号调整', '投注人数(结算)',
                    '投注额(结算)', '有效投注(结算)', '公司输赢含提前结算(结算)',
                    '红利', '返水', '代理佣金', '公司净收入', '集团分成(结算)']
    for col in numeric_cols:
        if col in grouped.columns:
            grouped[col] = grouped[col].apply(to_thousands_separator)

    # 映射站点ID
    grouped['站点ID'] = grouped['站点ID'].map(SITE_MAPPING)

    return grouped


async def job(bot):
    """主任务函数"""
    print(f"当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    with pymysql.connect(**DB_CONFIG) as conn:
        # 处理日报表
        daily_df = pd.read_sql_query("SELECT * FROM bigdata.platform_daily_report;", conn)
        daily_data = process_dataframe(daily_df, 'daily')
        daily_data.to_excel(f"{FOLDER_PATH}/平台报表-日报表.xlsx", index=False)

        # 处理月报表
        monthly_df = pd.read_sql_query("SELECT * FROM bigdata.platform_month_report;", conn)
        monthly_data = process_dataframe(monthly_df, 'monthly')
        monthly_data.to_excel(f"{FOLDER_PATH}/平台报表-月报表.xlsx", index=False)

    # 发送文件
    for file_name in os.listdir(FOLDER_PATH):
        file_path = os.path.join(FOLDER_PATH, file_name)
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'rb') as file:
                    await bot.send_document(chat_id=CHAT_ID, document=file)
                print(f"文件已发送: {file_name}")
            except TelegramError as e:
                print(f"发送文件 {file_name} 时出错: {e}")

    # 删除文件
    delete_files(FOLDER_PATH)


async def main():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    while True:
        await job(bot)
        await asyncio.sleep(3600)  # 每小时执行一次


if __name__ == "__main__":
    asyncio.run(main())
