import os
import asyncio
import time
import datetime
from telegram import Bot
from telegram.error import TelegramError
import pymysql
import pandas as pd
import pymongo
from pymongo import MongoClient

# 文件保存路径
folder_path = 'C:/Henvita/1_定时注单导出/收费站/'

# Telegram 配置
TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
CHAT_ID = '7523061850'  # 使用群聊 ID
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# 导出存款数据
def export_data_from_database_cunkuan():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    start_time = f"{yesterday} 00:00:00"
    end_time = f"{yesterday} 23:59:59"
    site_id = '2000'

    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='finance_1000'
    )

    query = f"SELECT * FROM finance_1000.finance_pay_records WHERE pay_status IN (2, 3) AND confirm_at BETWEEN '{start_time}' AND '{end_time}' AND site_id ='{site_id}';"
    df = pd.read_sql_query(query, connection)
    connection.close()

    df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '会员真实姓名', '会员等级', '上级ID', '账单号',
                  '通用支付订单号', '订单金额', '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例',
                  '支付状态', '客户端类型', '客户端IP', '充币名称', '收款人名称', '收款银行', '收款地址', '收款账号',
                  '收款码', '收款银行名称', '支付结果', '确认时间', '管理员ID', '创建时间', '更新时间', '支付渠道',
                  '支付渠道索引', '备注', '已支付金额', '已支付USDT金额', '订单状态', '转账账号', '充币账号', '财务备注',
                  '支付时间', '操作员', '完成时间', '订单来源', '商户号', '数据路由', '支付渠道操作员', '协议',
                  '预期USDT', '兑换USDT', 'APPID', '扩展字段1', '扩展字段2', '扩展字段3', '扩展字段8', '扩展字段11',
                  '扩展字段12', '扩展字段13', '扩展字段15', '信用评级', '信用等级', '奖励比例', '奖励金额', '提醒',
                  '请求来源', '设备号', '是否首充', '设备号成功次数', '客户端IP成功次数', '客户端IP位置', '客户端类型X']
    df = df[['站点ID', '会员ID', '会员用户名', '会员等级', '上级ID', '账单号', '通用支付订单号', '订单金额',
             '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例', '支付状态',
             '支付结果', '创建时间', '确认时间', '操作员', '完成时间']]

    df['支付方式'] = df['支付方式'].map({
        1001: "银行卡转账", 1002: "支付宝", 1003: "虚拟币扫码", 10205: "财务手动上分",
        891: "站点代客充值", 49999: "额度代存", 39999: "佣金代存", 890: "代客充值",
        1004: "数字人民币", 1005: "微信", 1006: "MPay", 1007: "银联快捷",
        1008: "IPay", 1009: "银联扫码", 1010: "云闪付扫码", 1011: "极速支付宝",
        1012: "极速数字人民币", 1013: "支付宝转卡", 1014: "云闪付转卡", 1015: "大额充值",
        1016: "京东支付", 1020: "支付宝h5", 1027: "FPAY钱包", 1028: "OKPAY钱包",
        1029: "TOPAY钱包", 1030: "GOPAY钱包", 1031: "808钱包", 1017: "支付宝小荷包",
        1018: "EBPay", 1019: "极速微信", 1021: "988钱包", 1022: "JD钱包",
        1023: "C币钱包", 1024: "K豆钱包", 1025: "钱能钱包", 1026: "TG钱包",
        1032: "网银转账", 1033: "万币钱包", 1034: "365钱包", 1035: "ABPAY钱包"
    })
    df['支付状态'] = df['支付状态'].map({1: "发起", 2: "确认", 3: "已对账", 4: "用户关闭", 5: "订单失效(45分钟)"})
    df['类别'] = df['类别'].map({0: "代客充值", 1: "会员存款", 2: "后台手动上分", 3: "佣金(代充)钱包转入充值", 6: "代理存款"})

    df.to_excel(folder_path + '存款单号.xlsx', index=False, engine='openpyxl')
    time.sleep(1)  # 确保文件关闭

# 导出提款数据
def export_data_from_database_qukuan():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    start_time = f"{yesterday} 00:00:00"
    end_time = f"{yesterday} 23:59:59"
    site_id = '2000'

    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='finance_1000'
    )

    query = f"SELECT * FROM finance_1000.finance_withdraw WHERE draw_status IN (402, 403) AND confirm_at BETWEEN '{start_time}' AND '{end_time}' and site_id = '{site_id}';"
    df = pd.read_sql_query(query, connection)
    connection.close()

    df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '手机号', '会员等级', '客户端类型', '客户端IP', '账单号',
                  '通用支付订单号', '订单金额', '实际支付金额', '预存款', '预存款时间', '预存款成功次数', '银行代码', '银行卡号',
                  '银行真实姓名', '类别', '提现状态', '风控管理员ID', '风控确认时间', '提现备注', '确认时间', '上级ID',
                  '创建时间', '更新时间', '银行地址', '自动风控结果', '风控备注', '拒绝原因', '拒绝内容', '暂扣原因',
                  '暂扣时间', '支付渠道', '提现类型', '支付渠道索引', '支付时间', '订单状态', '发货次数', '银行创建时间',
                  '风控管理员姓名', '暂扣姓名', '会员真实姓名', '风控操作员', '财务备注', '商户号', '数据路由', '设备号',
                  '风控接收时间', '协议', '兑换汇率', '预期币种', '手续费', '扩展字段0', '扩展字段1', '扩展字段2',
                  '扩展字段3', '扩展字段4', '扩展字段8', '信用评级', '信用等级', '提醒', '请求来源', '系统类型',
                  '系统类型信息', '支付组名称', '预提现', '操作员', '支付管理员ID', '奖励金额', '转账会员ID', '转账会员姓名',
                  '风控C管理员ID']
    df = df[['站点ID', '会员ID', '会员用户名', '会员等级', '账单号', '通用支付订单号', '订单金额', '实际支付金额',
             '预存款', '预存款时间', '预存款成功次数', '类别', '提现状态', '风控管理员ID', '风控确认时间', '提现备注',
             '确认时间', '上级ID', '创建时间', '更新时间', '自动风控结果', '风控备注', '拒绝原因', '拒绝内容',
             '暂扣原因', '暂扣时间', '支付渠道', '提现类型', '支付渠道索引', '支付时间', '订单状态', '发货次数',
             '银行创建时间', '风控管理员姓名', '风控操作员', '财务备注', '商户号', '设备号', '风控接收时间',
             '兑换汇率', '预期币种', '手续费', '信用评级', '信用等级', '提醒', '请求来源', '系统类型',
             '系统类型信息', '支付组名称', '预提现', '操作员', '支付管理员ID', '奖励金额', '转账会员ID', '转账会员姓名']]
    df['会员账号'] = df['会员用户名'].astype('str')

    df['提现状态'] = df['提现状态'].map({
        101: "发起，已扣款", 200: "风控计算流水中", 201: "自动风控不过，等待人工风控", 202: "人工审核挂起",
        300: "已风控,待付款", 401: "自动出款中", 402: "已付款，提款成功", 403: "已对账，提款成功",
        500: "已拒绝", 501: "出款失败"
    })
    df['类别'] = df['类别'].map({
        1: "会员中心钱包提款", 3: "代理钱包提款", 5: "虚拟币钱包提款", 11: "手动下分", 12: "代客下分"
    })
    df['提现类型'] = df['提现类型'].map({
        2001: "提款至银行卡", 20202: "提款至中心钱包", 20203: "佣金转账", 20204: "额度转账",
        20205: "额度代存", 20206: "佣金代存", 20207: "额度手动下分", 2002: "提款至虚拟币账户",
        20209: "代客提款", 1006: "Mpay钱包", 1008: "IPAY钱包", 1018: "EBPAY钱包",
        1021: "988钱包", 1022: "JD钱包", 1023: "C币钱包", 1024: "K豆钱包", 1025: "钱能钱包",
        1026: "TG钱包", 1027: "FPAY钱包", 1028: "OKPAY钱包", 1029: "TOPAY钱包",
        1030: "GOPAY钱包", 1031: "808钱包", 1033: "万币钱包", 1034: "365钱包", 1035: "ABPAY钱包"
    })

    df.to_excel(folder_path + '提款单号.xlsx', index=False, engine='openpyxl')
    time.sleep(1)

# 导出钱包数据
def export_data_from_database_wallet():
    site_id = 2000
    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='finance_1000'
    )
    query1 = "SELECT member_id, ANY_VALUE(available_money) AS 账户余额, ANY_VALUE(usdt_money) AS USDT余额 FROM member_wallet WHERE site_id = %s"
    balanced_record = pd.read_sql_query(query1, connection, params=(site_id,))
    connection.close()

    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='u1_1000'
    )
    query2 = "SELECT * FROM member_info WHERE site_id = %s;"
    member_info = pd.read_sql_query(query2, connection, params=(site_id,))
    connection.close()

    member_info.columns = ['member_id', '站点ID', '会员账号', '头像', '性别', '密码', '盐值', '状态',
                           '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称', '标签编号', '最后登录IP',
                           '注册设备ID', '最后登录设备ID', '注册IP', '最后登录时间', '来源链接', '邀请码',
                           '邀请码来源', '注册设备', '登录设备', '地址加密', '注册类型', '注册来源名称',
                           '注册来源代码', '应用ID', '手机号脱敏', '真实姓名脱敏', '邮箱脱敏', '注册IP加密',
                           '注册IP脱敏', '最后登录IP加密', '最后登录IP脱敏', '家庭电话脱敏', 'xs_s0',
                           'xs_s1', 'xs_s5', '标签名称', '标签', 'VIP等级', '登录认证', '手机实名验证',
                           'SVIP', '省份加密', '注册时间', '更新时间']
    member_info = member_info[['member_id', '站点ID', '会员账号', '性别', '状态', '代理ID', '上级代理',
                               '是否代理', '生日', '地区代码', '昵称', '最后登录时间', '来源链接', '注册设备',
                               '登录设备', 'VIP等级', '手机实名验证', 'SVIP', '注册时间']]

    balance_info = pd.merge(member_info, balanced_record, on='member_id', how='left')
    balance_info.to_excel(folder_path + '账户余额.xlsx', index=False, engine='openpyxl')
    time.sleep(1)

# 导出会员数据
def export_data_from_database_memberinfo():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    start_time = f"{yesterday} 00:00:00"
    end_time = f"{yesterday} 23:59:59"
    site_id = '2000'

    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='bigdata'
    )
    query1 = """
    SELECT member_id, ANY_VALUE(first_deposit_amount) AS 首存金额, ANY_VALUE(first_deposit_time) AS 首存时间
    FROM (SELECT * FROM member_daily_statics ORDER BY statics_date DESC) AS sorted_table GROUP BY member_id
    """
    last_record = pd.read_sql_query(query1, connection)

    query2 = """
    SELECT member_id, ANY_VALUE(statics_date) AS 最后投注日期
    FROM (SELECT * FROM member_daily_statics WHERE bet_amount > 0 ORDER BY statics_date DESC) AS sorted_table GROUP BY member_id
    """
    last_bet_date_data = pd.read_sql_query(query2, connection)

    query3 = f"""
    SELECT member_id, SUM(valid_bet_amount) 有效投注额, SUM(net_amount) 会员输赢, SUM(deposit_count) 存款次数,
    SUM(deposit) 存款额, SUM(draw_count) 提款次数, SUM(draw) 提款额, SUM(promo) 红利, SUM(rebate) 返水
    FROM member_daily_statics WHERE statics_date BETWEEN '{yesterday}' AND '{today}' GROUP BY member_id;
    """
    statics_data = pd.read_sql_query(query3, connection)
    connection.close()

    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='u1_1000'
    )
    query = "SELECT * FROM member_info;"
    member_info = pd.read_sql_query(query, connection)
    connection.close()

    member_info.columns = ['member_id', '站点ID', '会员账号', '头像', '性别', '密码', '盐值', '状态',
                           '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称', '标签编号', '最后登录IP',
                           '注册设备ID', '最后登录设备ID', '注册IP', '最后登录时间', '来源链接', '邀请码',
                           '邀请码来源', '注册设备', '登录设备', '地址加密', '注册类型', '注册来源名称',
                           '注册来源代码', '应用ID', '手机号脱敏', '真实姓名脱敏', '邮箱脱敏', '注册IP加密',
                           '注册IP脱敏', '最后登录IP加密', '最后登录IP脱敏', '家庭电话脱敏', 'xs_s0',
                           'xs_s1', 'xs_s5', '标签名称', '标签', 'VIP等级', '登录认证', '手机实名验证',
                           'SVIP', '省份加密', '注册时间', '更新时间']
    member_info = member_info[['member_id', '站点ID', '会员账号', '性别', '状态', '代理ID', '上级代理',
                               '是否代理', '生日', '地区代码', '昵称', '标签编号', '最后登录时间', '来源链接',
                               '注册设备', '登录设备', 'VIP等级', '登录认证', '手机实名验证', 'SVIP', '注册时间']]

    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='control_1000'
    )
    query = "SELECT code, dict_value FROM sys_dict_value;"
    dict_value = pd.read_sql_query(query, connection)
    dictionary = dict(zip(dict_value['code'], dict_value['dict_value']))
    connection.close()

    def replace_labels(label_string):
        labels = str(label_string).split(',')
        replaced_labels = [dictionary.get(label, label) for label in labels]
        return ','.join(replaced_labels)

    member_info['风控标签'] = member_info['标签编号'].apply(replace_labels)

    client = MongoClient("mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/")
    db = client["update_records"]
    collections = [col for col in db.list_collection_names() if col.startswith('pull_order')]
    aggregation_results = []

    for collection_name in collections:
        collection = db[collection_name]
        pipeline = [
            {"$match": {"flag": 1, "settle_time": {"$gte": start_time, "$lte": end_time}}},
            {"$project": {"member_id": 1, "game_type": 1, "valid_bet_amount": 1, "net_amount": 1}},
            {"$group": {
                "_id": {"member_id": "$member_id", "game_type": "$game_type"},
                "total_valid_bet_amount": {"$sum": "$valid_bet_amount"},
                "total_net_amount": {"$sum": "$net_amount"}
            }}
        ]
        result = collection.aggregate(pipeline)
        for doc in result:
            aggregation_results.append({
                "member_id": doc["_id"]["member_id"],
                "game_type": doc["_id"]["game_type"],
                "total_valid_bet_amount": doc["total_valid_bet_amount"],
                "total_net_amount": doc["total_net_amount"]
            })

    type_data = pd.DataFrame(aggregation_results)
    game_type_mapping_net = {1: '体育会员输赢', 2: '电竞会员输赢', 3: '真人会员输赢', 4: '彩票会员输赢',
                             5: '棋牌会员输赢', 6: '电子会员输赢', 7: '捕鱼会员输赢'}
    game_type_mapping_valid = {1: '体育有效投注', 2: '电竞有效投注', 3: '真人有效投注', 4: '彩票有效投注',
                               5: '棋牌有效投注', 6: '电子有效投注', 7: '捕鱼有效投注'}

    type_data_pivot = type_data.pivot_table(index='member_id', columns='game_type',
                                            values='total_net_amount', aggfunc='sum').reset_index()
    type_data_pivot.columns = ['member_id'] + [game_type_mapping_net.get(col, col) for col in type_data_pivot.columns[1:]]

    type_data_valid_pivot = type_data.pivot_table(index='member_id', columns='game_type',
                                                  values='total_valid_bet_amount', aggfunc='sum').reset_index()
    type_data_valid_pivot.columns = ['member_id'] + [game_type_mapping_valid.get(col, col) for col in type_data_valid_pivot.columns[1:]]

    member_info = pd.merge(member_info, last_record, on='member_id', how='left')
    member_info = pd.merge(member_info, last_bet_date_data, on='member_id', how='left')
    member_info = pd.merge(member_info, statics_data, on='member_id', how='left')
    member_info = pd.merge(member_info, type_data_pivot, on='member_id', how='left')
    member_info = pd.merge(member_info, type_data_valid_pivot, on='member_id', how='left')

    connection = pymysql.connect(
        host='18.178.159.230', port=3366, user='bigdata',
        password='uvb5SOSmLH8sCoSU', database='u1_1000'
    )
    query = "SELECT member_id, member_credit_level FROM finance_1000.finance_member_level"
    member_credit_level = pd.read_sql_query(query, connection)
    member_credit_level.columns = ['member_id', '会员信用层级']
    connection.close()

    member_info = pd.merge(member_info, member_credit_level, on='member_id', how='left')
    filtered_member_info = member_info[member_info['站点ID'] == 2000]
    filtered_member_info['会员信用层级'] = filtered_member_info['会员信用层级'].fillna('0_0')

    filtered_member_info.to_excel(folder_path + '会员报表.xlsx', index=False, engine='openpyxl')
    time.sleep(1)

# 删除目录下所有文件
def delete_all_files_in_directory(directory):
    if os.path.exists(directory) and os.path.isdir(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                for _ in range(5):
                    try:
                        os.remove(file_path)
                        print(f"删除文件: {file_path}")
                        break
                    except PermissionError:
                        print(f"文件 {file_path} 被占用，等待 1 秒后重试...")
                        time.sleep(1)
                else:
                    print(f"无法删除文件 {file_path}，请手动检查。")
    else:
        print("指定的目录不存在或不是一个目录。")

# 发送文件夹中的文件到 Telegram
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
            await asyncio.sleep(2)  # 增加 2 秒延迟，避免连接池超载

# 计算到目标时间的等待秒数
def get_time_until(target_hour, target_minute):
    now = datetime.datetime.now()
    next_target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if now >= next_target_time:
        next_target_time += datetime.timedelta(days=1)
    seconds_until_next_target = (next_target_time - now).total_seconds()
    print(f"下一次执行将在 {next_target_time.strftime('%Y-%m-%d %H:%M:%S')} 发生。")
    return seconds_until_next_target

# 定时任务：存款、提款、会员数据
async def schedule_cunkuan_qukuan():
    while True:
        await asyncio.sleep(get_time_until(11, 20))
        try:
            export_data_from_database_cunkuan()
            export_data_from_database_qukuan()
            export_data_from_database_memberinfo()
            await send_files_in_folder(bot, folder_path, CHAT_ID)
            delete_all_files_in_directory(folder_path)
        except Exception as e:
            print(f"存款/提款任务执行出错: {e}")

# 定时任务：钱包数据
async def schedule_wallet():
    while True:
        await asyncio.sleep(get_time_until(13, 3))
        try:
            export_data_from_database_wallet()
            await send_files_in_folder(bot, folder_path, CHAT_ID)
            delete_all_files_in_directory(folder_path)
        except Exception as e:
            print(f"钱包任务执行出错: {e}")

# 主函数
async def main():
    task_cunkuan_qukuan = asyncio.create_task(schedule_cunkuan_qukuan())
    task_wallet = asyncio.create_task(schedule_wallet())
    await asyncio.gather(task_cunkuan_qukuan, task_wallet)

# 启动程序
def run_bot():
    asyncio.run(main())

if __name__ == "__main__":
    run_bot()
