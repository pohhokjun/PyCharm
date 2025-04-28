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
SITE_MAPPING = {
    1000: "好博体育", 2000: "黄金体育", 3000: "宾利体育", 4000: "HOME体育",
    5000: "亚洲之星", 6000: "玖博体育", 7000: "蓝火体育", 8000: "A7体育",
    9000: "K9体育", 9001: "摩根体育", 9002: "幸运体育"
}


def to_thousands(number):
    return f"{number:,.0f}"


def delete_files(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"删除文件: {file_path}")


def process_dataframe(df, period='daily'):
    group_key = '日期' if period == 'daily' else '月份'
    columns = ['id', group_key, '站点ID', '渠道类型'] + [
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

    agg_cols = {
        '注册人数': 'sum', '首存人数': 'sum', '首存金额': 'sum', '存款人数': 'sum',
        '充值金额': 'sum', '提款人数': 'sum', '提款金额': 'sum', '存提差': 'sum',
        '账号调整': 'sum', '投注人数(结算)': 'sum', '投注额(结算)': 'sum',
        '有效投注(结算)': 'sum', '公司输赢含提前结算(结算)': 'sum', '红利': 'sum',
        '返水': 'sum', '个人佣金金额': 'sum', '团队佣金金额': 'sum'
    }
    grouped = df.groupby(['站点ID', group_key])[list(agg_cols.keys())].sum().reset_index()

    grouped['集团分成(结算)'] = (grouped['公司输赢含提前结算(结算)'] + grouped['账号调整']) * 0.12
    grouped['代理佣金'] = grouped['个人佣金金额'] + grouped['团队佣金金额']
    grouped['公司净收入'] = (grouped['公司输赢含提前结算(结算)'] + grouped['账号调整'] -
                             grouped['红利'] - grouped['返水'] - grouped['代理佣金'] -
                             grouped['集团分成(结算)'])
    grouped.drop(columns=['个人佣金金额', '团队佣金金额'], inplace=True)

    current_month = datetime.datetime.now().strftime("%Y-%m")
    grouped = grouped[grouped[group_key].str.startswith(current_month)]

    numeric_cols = list(agg_cols.keys()) + ['代理佣金', '公司净收入', '集团分成(结算)']
    numeric_cols = [col for col in numeric_cols if col in grouped.columns]
    for col in numeric_cols:
        grouped[col] = grouped[col].apply(to_thousands)

    grouped['站点ID'] = grouped['站点ID'].map(SITE_MAPPING)
    return grouped


async def job(bot):
    print(f"当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    with pymysql.connect(**DB_CONFIG) as conn:
        for period, table, file_name in [
            ('daily', 'platform_daily_report', '平台报表-日报表.xlsx'),
            ('monthly', 'platform_month_report', '平台报表-月报表.xlsx')
        ]:
            df = pd.read_sql_query(f"SELECT * FROM bigdata.{table};", conn)
            data = process_dataframe(df, period)

            writer = pd.ExcelWriter(f"{FOLDER_PATH}/{file_name}", engine='xlsxwriter')
            workbook = writer.book

            header_format = workbook.add_format({'bg_color': '#1E3A8A', 'font_color': '#FFFFFF', 'bold': True})
            even_row_format = workbook.add_format({'bg_color': '#60A5FA'})

            if period == 'daily':
                for site in data['站点ID'].unique():
                    site_data = data[data['站点ID'] == site]
                    site_data.to_excel(writer, sheet_name=site, index=False)

                    worksheet = writer.sheets[site]
                    col_count = site_data.shape[1]

                    # 首行格式
                    for col in range(col_count):
                        worksheet.write(0, col, site_data.columns[col], header_format)

                    # 交替行颜色
                    worksheet.conditional_format(1, 0, site_data.shape[0], col_count - 1,
                                                 {'type': 'formula',
                                                  'criteria': 'MOD(ROW(),2)=0',
                                                  'format': even_row_format})

                    # 自动调整列宽
                    worksheet.autofit()

                    worksheet.freeze_panes(1, 0)
            else:
                data.to_excel(writer, sheet_name='月报', index=False)
                worksheet = writer.sheets['月报']
                col_count = data.shape[1]

                # 首行格式
                for col in range(col_count):
                    worksheet.write(0, col, data.columns[col], header_format)

                # 交替行颜色
                worksheet.conditional_format(1, 0, data.shape[0], col_count - 1,
                                             {'type': 'formula',
                                              'criteria': 'MOD(ROW(),2)=0',
                                              'format': even_row_format})

                # 自动调整列宽
                worksheet.autofit()

                worksheet.freeze_panes(1, 0)

            writer.close()

    for file_name in os.listdir(FOLDER_PATH):
        file_path = os.path.join(FOLDER_PATH, file_name)
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'rb') as file:
                    await bot.send_document(chat_id=CHAT_ID, document=file)
                print(f"文件已发送: {file_name}")
            except TelegramError as e:
                print(f"发送文件 {file_name} 时出错: {e}")

    delete_files(FOLDER_PATH)


async def main():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    while True:
        await job(bot)
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())