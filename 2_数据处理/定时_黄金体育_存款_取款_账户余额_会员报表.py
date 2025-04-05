import os
import asyncio
from telegram import Bot
from telegram.error import TelegramError
import pymysql
import pandas as pd
import pymongo
import datetime

folder_path = 'C:/Henvita/1_定时注单导出/收费站'

def export_data_from_database_cunkuan():

    today = datetime.datetime.now().strftime('%Y-%m-%d')  # 今天日期
    print(today)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # 昨天日期
    print(yesterday)
    start_time = str(yesterday) + ' 00:00:00'  # 开始时间
    end_time = str(yesterday) + ' 23:59:59'  # 结束时间
    site_id = '2000'  # 站点ID

    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='finance_1000'
    )

    # 使用f-string格式化查询语句
    query = f"SELECT * FROM finance_1000.finance_pay_records WHERE pay_status IN (2, 3) AND confirm_at BETWEEN '{start_time}' AND '{end_time}' AND site_id ='{site_id}';"

    df = pd.read_sql_query(query, connection)
    print(df.columns)
    print(df)

    # ['id', 'site_id', 'member_id', 'member_username', 'member_real_name',
    #        'member_grade', 'top_id', 'bill_no', 'typay_order_id', 'order_amount',
    #        'pay_amount', 'pay_seq', 'rebate_amount', 'score_amount', 'category',
    #        'pay_type', 'flow_ratio', 'pay_status', 'client_type', 'client_ip',
    #        'desposit_name', 'recipient_name', 'recipient_bank',
    #        'recipient_address', 'recipient_account', 'recipient_code',
    #        'recipient_bank_name', 'pay_result', 'confirm_at', 'admin_id',
    #        'created_at', 'updated_at', 'pay_channel', 'pay_channel_index',
    #        'remark', 'paid_amount', 'paid_usdt_amount', 'order_status',
    #        'transfer_account', 'deposit_account', 'finance_remark', 'pay_time',
    #        'operator', 'complete_time', 'order_source', 'merchant_no',
    #        'data_route', 'pay_operator', 'protocol', 'expected_usdt',
    #        'exchange_usdt', 'app_id', 'xs_s1', 'xs_s2', 'xs_s3', 'xs_s8', 'xs_s11',
    #        'xs_s12', 'xs_s13', 'xs_s15', 'credit_rating', 'credit_level',
    #        'reward_rate', 'reward_amount', 'reminder', 'request_source',
    #        'device_no', 'is_first_deposit', 'device_no_success_count',
    #        'client_ip_success_count', 'client_ip_location', 'client_type_x', 'rn']

    connection.close()
    df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '会员真实姓名', '会员等级', '上级ID', '账单号',
                  '通用支付订单号',
                  '订单金额', '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例',
                  '支付状态', '客户端类型', '客户端IP', '充币名称', '收款人名称', '收款银行', '收款地址', '收款账号',
                  '收款码', '收款银行名称', '支付结果', '确认时间', '管理员ID', '创建时间', '更新时间', '支付渠道',
                  '支付渠道索引', '备注', '已支付金额', '已支付USDT金额', '订单状态', '转账账号', '充币账号',
                  '财务备注',
                  '支付时间', '操作员', '完成时间', '订单来源', '商户号', '数据路由', '支付渠道操作员', '协议',
                  '预期USDT',
                  '兑换USDT', 'APPID', '扩展字段1', '扩展字段2', '扩展字段3', '扩展字段8', '扩展字段11', '扩展字段12',
                  '扩展字段13', '扩展字段15', '信用评级', '信用等级', '奖励比例', '奖励金额', '提醒', '请求来源',
                  '设备号',
                  '是否首充', '设备号成功次数', '客户端IP成功次数', '客户端IP位置', '客户端类型X']
    df = df[['站点ID', '会员ID', '会员用户名', '会员等级', '上级ID', '账单号', '通用支付订单号', '订单金额',
             '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例',
             '支付状态', '支付结果', '创建时间', '确认时间', '操作员', '完成时间']]

    # 输出数据到excel文件
    df['支付方式'] = df['支付方式'].map({
        1001: "银行卡转账",
        1002: "支付宝",
        1003: "虚拟币扫码",
        10205: "财务手动上分",
        891: "站点代客充值",
        49999: "额度代存",
        39999: "佣金代存",
        890: "代客充值",
        1004: "数字人民币",
        1005: "微信",
        1006: "MPay",
        1007: "银联快捷",
        1008: "IPay",
        1009: "银联扫码",
        1010: "云闪付扫码",
        1011: "极速支付宝",
        1012: "极速数字人民币",
        1013: "支付宝转卡",
        1014: "云闪付转卡",
        1015: "大额充值",
        1016: "京东支付",
        1020: "支付宝h5",
        1027: "FPAY钱包",
        1028: "OKPAY钱包",
        1029: "TOPAY钱包",
        1030: "GOPAY钱包",
        1031: "808钱包",
        1017: "支付宝小荷包",
        1018: "EBPay",
        1019: "极速微信",
        1021: "988钱包",
        1022: "JD钱包",
        1023: "C币钱包",
        1024: "K豆钱包",
        1025: "钱能钱包",
        1026: "TG钱包",
        1032: "网银转账",
        1033: "万币钱包",
        1034: "365钱包",
        1035: "ABPAY钱包"
    })

    # 输出数据到excel文件
    df['支付状态'] = df['支付状态'].map({1: "发起", 2: "确认", 3: "已对账", 4: "用户关闭", 5: "订单失效(45分钟)"})

    # 输出数据到excel文件
    df['类别'] = df['类别'].map(
        {0: "代客充值", 1: "会员存款", 2: "后台手动上分", 3: "佣金(代充)钱包转入充值", 6: "代理存款"})

    def select(data, column, start_time, end_time):
        data = data[(data[column] >= start_time) & (data[column] < end_time)]
        return data

    # df = select(df, '确认时间', '2024-12-01', '2024-12-02')
    df.to_excel(folder_path + '存款单号.xlsx', index=False)

# ----------------------------------------------------------------------------------------------------------------------
def export_data_from_database_qukuan():

    today = datetime.datetime.now().strftime('%Y-%m-%d')  # 今天日期
    print(today)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # 昨天日期
    print(yesterday)
    start_time = str(yesterday) + ' 00:00:00'  # 开始时间
    end_time = str(yesterday) + ' 23:59:59'  # 结束时间
    site_id = '2000'  # 站点ID

    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='finance_1000'
    )

    # 使用f-string格式化查询语句
    query = f"SELECT * FROM finance_1000.finance_withdraw WHERE draw_status IN (402, 403) AND confirm_at BETWEEN '{start_time}' AND '{end_time}' and site_id = '{site_id}';"

    df = pd.read_sql_query(query, connection)
    print(df.columns)
    print(df)

    # Index(['id', 'site_id', 'member_id', 'member_username', 'phone',
    #        'member_grade', 'client_type', 'client_ip', 'bill_no', 'typay_order_id',
    #        'amount', 'usdt_amount', 'preview_deposit', 'preview_deposit_time',
    #        'preview_success_num', 'bank_code', 'bank_card', 'bank_realname',
    #        'category', 'draw_status', 'risk_admin_id', 'risk_confirm_at',
    #        'draw_comment', 'confirm_at', 'top_id', 'created_at', 'updated_at',
    #        'bank_address', 'auto_risk_result', 'risk_comment', 'denial_reason',
    #        'denial_content', 'hold_reason', 'hold_at', 'pay_channel',
    #        'withdraw_type', 'pay_channel_index', 'payment_time', 'order_status',
    #        'send_count', 'bank_created_at', 'risk_admin_name', 'hold_name',
    #        'member_real_name', 'risk_operater', 'finance_remark', 'merchant_no',
    #        'data_route', 'device_no', 'risk_receive_at', 'protocol',
    #        'exchange_rate', 'expected_digiccy', 'handling_fee', 'xs_s0', 'xs_s1',
    #        'xs_s2', 'xs_s3', 'xs_s4', 'xs_s8', 'credit_rating', 'credit_level',
    #        'reminder', 'request_source', 'sys_type', 'sys_type_info',
    #        'payment_group_name', 'pre_withdraw', 'operator', 'pay_admin_id',
    #        'reward_amount', 'transfer_member_id', 'transfer_member_name'], 'risk_c_admin_id'],
    #       dtype='object')

    df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '手机号', '会员等级', '客户端类型', '客户端IP', '账单号',
                  '通用支付订单号',
                  '订单金额', '实际支付金额', '预存款', '预存款时间', '预存款成功次数', '银行代码', '银行卡号',
                  '银行真实姓名',
                  '类别', '提现状态', '风控管理员ID', '风控确认时间', '提现备注', '确认时间', '上级ID', '创建时间',
                  '更新时间',
                  '银行地址', '自动风控结果', '风控备注', '拒绝原因', '拒绝内容', '暂扣原因', '暂扣时间', '支付渠道',
                  '提现类型', '支付渠道索引', '支付时间', '订单状态', '发货次数', '银行创建时间', '风控管理员姓名',
                  '暂扣姓名',
                  '会员真实姓名', '风控操作员', '财务备注', '商户号', '数据路由', '设备号', '风控接收时间', '协议',
                  '兑换汇率', '预期币种', '手续费', '扩展字段0', '扩展字段1', '扩展字段2', '扩展字段3', '扩展字段4',
                  '扩展字段8', '信用评级', '信用等级', '提醒', '请求来源', '系统类型', '系统类型信息',
                  '支付组名称', '预提现', '操作员', '支付管理员ID', '奖励金额', '转账会员ID', '转账会员姓名', '风控C管理员ID']

    df = df[['站点ID', '会员ID', '会员用户名', '会员等级', '账单号', '通用支付订单号',
             '订单金额', '实际支付金额', '预存款', '预存款时间', '预存款成功次数',
             '类别', '提现状态', '风控管理员ID', '风控确认时间', '提现备注', '确认时间', '上级ID', '创建时间',
             '更新时间',
             '自动风控结果', '风控备注', '拒绝原因', '拒绝内容', '暂扣原因', '暂扣时间', '支付渠道',
             '提现类型', '支付渠道索引', '支付时间', '订单状态', '发货次数', '银行创建时间', '风控管理员姓名',
             '风控操作员', '财务备注', '商户号', '设备号', '风控接收时间',
             '兑换汇率', '预期币种', '手续费',
             '信用评级', '信用等级', '提醒', '请求来源', '系统类型', '系统类型信息',
             '支付组名称', '预提现', '操作员', '支付管理员ID', '奖励金额', '转账会员ID', '转账会员姓名']]
    df['会员账号'] = df['会员用户名'].astype('str')

    # 输出数据到excel文件
    df['提现状态'] = df['提现状态'].map({
        101: "发起，已扣款",
        200: "风控计算流水中",
        201: "自动风控不过，等待人工风控",
        202: "人工审核挂起",
        300: "已风控,待付款",
        401: "自动出款中",
        402: "已付款，提款成功",
        403: "已对账，提款成功",
        500: "已拒绝",
        501: "出款失败",
    })

    # 输出数据到excel文件
    df['类别'] = df['类别'].map({
        1: "会员中心钱包提款",
        3: "代理钱包提款",
        5: "虚拟币钱包提款",
        11: "手动下分",
        12: "代客下分",
    })

    df['提现类型'] = df['提现类型'].map({
        2001: "提款至银行卡",
        20202: "提款至中心钱包",
        20203: "佣金转账",
        20204: "额度转账",
        20205: "额度代存",
        20206: "佣金代存",
        20207: "额度手动下分",
        2002: "提款至虚拟币账户",
        20209: "代客提款",
        1006: "Mpay钱包",
        1006: "Mpay钱包",
        1008: "IPAY钱包",
        1018: "EBPAY钱包",
        1021: "988钱包",
        1022: "JD钱包",
        1023: "C币钱包",
        1024: "K豆钱包",
        1025: "钱能钱包",
        1026: "TG钱包",
        1027: "FPAY钱包",
        1028: "OKPAY钱包",
        1029: "TOPAY钱包",
        1030: "GOPAY钱包",
        1031: "808钱包",
        1033: "万币钱包",
        1034: "365钱包",
        1035: "ABPAY钱包",
    })

    connection.close()

    df.to_excel(folder_path + '提款单号.xlsx', index=False)

# ----------------------------------------------------------------------------------------------------------------------
def export_data_from_database_wallet():
    site_id = 2000
    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='finance_1000'
    )
    query1 = """
    SELECT member_id,
    ANY_VALUE(available_money) AS 账户余额,
    ANY_VALUE(usdt_money) AS USDT余额
    FROM member_wallet where site_id = %s"""

    balanced_record = pd.read_sql_query(query1, connection, params=(site_id,))
    print(balanced_record.columns)
    print(balanced_record.head())

    # df.columns = ['编号', '站点编号', '账户余额', '冻结金额', '代理余额',
    #               '代理冻结金额', '代操金额', '系统类型', '系统状态',
    #               '创建时间', '更新时间', '会员编号', 'USDT余额',
    #               'USDT冻结金额']

    connection.close()

    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='u1_1000'
    )

    query2 = """
    SELECT * from member_info where site_id = %s;
    """
    member_info = pd.read_sql_query(query2, connection, params=(site_id,))
    print(member_info.head())
    connection.close()

    member_info.columns = ['member_id', '站点ID', '会员账号', '头像', '性别', '密码', '盐值', '状态',
                           '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称',
                           '标签编号', '最后登录IP', '注册设备ID', '最后登录设备ID',
                           '注册IP', '最后登录时间', '来源链接', '邀请码',
                           '邀请码来源', '注册设备', '登录设备', '地址加密',
                           '注册类型', '注册来源名称', '注册来源代码',
                           '应用ID', '手机号脱敏', '真实姓名脱敏',
                           '邮箱脱敏', '注册IP加密',
                           '注册IP脱敏', '最后登录IP加密',
                           '最后登录IP脱敏', '家庭电话脱敏', 'xs_s0',
                           'xs_s1', 'xs_s5', '标签名称', '标签', 'VIP等级', '登录认证',
                           '手机实名验证', 'SVIP', '省份加密', '注册时间',
                           '更新时间']

    member_info = member_info[['member_id', '站点ID', '会员账号', '性别', '状态',
                               '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称',
                               '最后登录时间', '来源链接', '注册设备', '登录设备',
                               'VIP等级', '手机实名验证', 'SVIP', '注册时间']]

    balance_info = pd.merge(member_info, balanced_record, on='member_id', how='left')
    print(balance_info.head())
    balance_info.to_excel(folder_path + '账户余额.xlsx', index=False)

# ----------------------------------------------------------------------------------------------------------------------
def export_data_from_database_memberinfo():
    # import pandas as pd

    today = datetime.datetime.now().strftime('%Y-%m-%d')  # 今天日期
    print(today)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # 昨天日期
    print(yesterday)
    start_time = str(yesterday) + ' 00:00:00'  # 开始时间
    end_time = str(yesterday) + ' 23:59:59'  # 结束时间
    site_id = '2000'  # 站点ID

    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='bigdata'
    )

    query1 = """
    SELECT member_id, 
           ANY_VALUE(first_deposit_amount) AS 首存金额, 
           ANY_VALUE(first_deposit_time) AS  首存时间
    FROM (
        SELECT *
        FROM member_daily_statics
        ORDER BY statics_date DESC
    ) AS sorted_table
    GROUP BY member_id
    """

    last_record = pd.read_sql_query(query1, connection)
    print(last_record.columns)

    query2 = """
    SELECT member_id, 
           ANY_VALUE(statics_date) AS 最后投注日期
    FROM (
        SELECT *
        FROM member_daily_statics
        where bet_amount > 0
        ORDER BY statics_date DESC
    ) AS sorted_table
    GROUP BY member_id
    """

    last_bet_date_data = pd.read_sql_query(query2, connection)

    start_time = str(yesterday)
    end_time = str(today)

    query3 = f"""SELECT member_id, sum(valid_bet_amount) 有效投注额, sum(net_amount) 会员输赢, sum(deposit_count) 存款次数,
    sum(deposit)存款额, sum(draw_count) 提款次数,sum(draw)提款额,  sum(promo) 红利, sum(rebate) 返水 from member_daily_statics where statics_date between '{start_time}'
    and '{end_time}' GROUP BY member_id;"""

    statics_data = pd.read_sql_query(query3, connection)

    connection.close()

    # df.columns = ['ID', '站点ID', '统计日期', '会员ID', '会员账号', '上级ID',
    #               '存款次数', '存款金额', '提款金额', '提款次数', '优惠', '返水',
    #               '分数调整', '存款调整金额', '投注', '利润', '公司输赢', '提前结算金额',
    #               '创建时间', '更新时间', '系统类型', '注册时间', '注册来源', '注册链接',
    #               '地区代码', '地区名称', '首存金额', '首存时间', '首投金额', '代理佣金',
    #               '上级名称', '风险调整', '总投注', '是否新用户', '输赢调整金额', '有效投注金额',
    #               '投注金额', '会员输赢', '提前结算金额', '系统调整金额', '存款手续费', '提款手续费', '打赏金额']

    # df.to_excel('会员每日数据.xlsx', index=False)

    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='u1_1000'
    )

    query = """
    SELECT * from member_info;
    """

    member_info = pd.read_sql_query(query, connection)
    print(member_info.head())

    connection.close()

    member_info.columns = ['member_id', '站点ID', '会员账号', '头像', '性别', '密码', '盐值', '状态',
                           '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称',
                           '标签编号', '最后登录IP', '注册设备ID', '最后登录设备ID',
                           '注册IP', '最后登录时间', '来源链接', '邀请码',
                           '邀请码来源', '注册设备', '登录设备', '地址加密',
                           '注册类型', '注册来源名称', '注册来源代码',
                           '应用ID', '手机号脱敏', '真实姓名脱敏',
                           '邮箱脱敏', '注册IP加密',
                           '注册IP脱敏', '最后登录IP加密',
                           '最后登录IP脱敏', '家庭电话脱敏', 'xs_s0',
                           'xs_s1', 'xs_s5', '标签名称', '标签', 'VIP等级', '登录认证',
                           '手机实名验证', 'SVIP', '省份加密', '注册时间',
                           '更新时间']

    member_info = member_info[['member_id', '站点ID', '会员账号', '性别', '状态',
                               '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称',
                               '标签编号', '最后登录时间', '来源链接', '注册设备', '登录设备',
                               'VIP等级', '登录认证', '手机实名验证', 'SVIP', '注册时间']]

    # connection = pymysql.connect(
    #     host='18.178.159.230',
    #     port=3366,
    #     user='bigdata',
    #     password='uvb5SOSmLH8sCoSU',
    #     database='agent_1000'
    # )
    #
    # query = """
    # SELECT t1.member_id, t1.agent_name, t2.group_name, t2.level FROM
    # (agent_1000.agent_dept_member t1
    # left join agent_1000.agent_department t2
    # on t1.dept_id = t2.pid);
    # """
    #
    # level_data = pd.read_sql_query(query, connection)
    # print(level_data.columns)
    # level_data.columns = ['代理ID', '上级代理', '名称', 'level']
    #
    # connection.close()
    #
    # level1 = level_data[level_data['level'] == 1]
    # level2 = level_data[level_data['level'] == 2]
    # level3 = level_data[level_data['level'] == 3]
    # level4 = level_data[level_data['level'] == 4]

    member_info = pd.merge(member_info, last_record, on='member_id', how='left')
    member_info = pd.merge(member_info, last_bet_date_data, on='member_id', how='left')
    member_info = pd.merge(member_info, statics_data, on='member_id', how='left')
    # member_info = pd.merge(member_info, level1, on=['代理ID'], how='left')
    # member_info = pd.merge(member_info, level2, on=['代理ID'], how='left', suffixes=('一级','二级'))
    # member_info = pd.merge(member_info, level3, on=['代理ID'], how='left',suffixes=('_1', '_2'))
    # member_info = pd.merge(member_info, level4, on=['代理ID'], how='left',suffixes=('三级','四级'))
    # print(member_info.columns)

    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='control_1000'
    )

    query = """
    SELECT code,dict_value from sys_dict_value;
    """

    dict_value = pd.read_sql_query(query, connection)
    print(dict_value.columns)
    # 请把dict_value的code和dict_value的dict_value合并成一个字典
    # 例如：
    # code = ['1', '2', '3']
    # dict_value = ['男', '女', '未知']
    # dict = {'1': '男', '2': '女', '3': '未知'}
    # 这样就可以用dict[code]来获取对应的dict_value值了
    dictionary = dict(zip(dict_value['code'], dict_value['dict_value']))

    connection.close()

    def replace_labels(label_string):
        labels = label_string.split(',')
        replaced_labels = [dictionary.get(label, label) for label in labels]
        return ','.join(replaced_labels)

    member_info['风控标签'] = member_info['标签编号'].apply(replace_labels)

    # member_info.to_excel('会员累计数据-最后投注日期.xlsx', index=False)

    # import pymongo
    # from pymongo import MongoClient
    # from datetime import datetime
    # import pandas as pd

    # 设置 MongoDB 连接
    client = pymongo.MongoClient("mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/")  # 根据实际连接信息修改
    db = client["update_records"]  # 切换到目标数据库
    start_time = start_time + " 00:00:00"
    end_time = end_time + " 00:00:00"
    # 获取所有以 "pull_order" 开头的 collection
    collections = [col for col in db.list_collection_names() if col.startswith('pull_order')]

    # 聚合结果
    aggregation_results = []

    # 遍历每个 collection
    for collection_name in collections:
        collection = db[collection_name]

        # 聚合查询：筛选 flag=1 的数据，按 settle_time, member_id, venue_name, game_name 分组
        pipeline = [
            {
                "$match": {
                    "flag": 1,  # 只选择 flag=1 的数据
                    "settle_time": {"$gte": start_time, "$lte": end_time}  # 时间范围筛选
                }
            },
            {
                "$project": {
                    # "settle_time": 1,
                    "member_id": 1,
                    "game_type": 1,
                    # "venue_name": 1,
                    # "game_name": 1,
                    # "bet_amount": 1,
                    "valid_bet_amount": 1,
                    "net_amount": 1,
                    # "id": 1
                }
            },
            {
                "$group": {
                    "_id": {
                        # "date": {"$substr": ["$settle_time", 0, 10]},  # 提取前 7 位（如 2024-10） # 按日期分组
                        "member_id": "$member_id",
                        "game_type": "$game_type",
                        # "venue_name": "$venue_name",
                        # "game_name": "$game_name"
                    },
                    # "betting_count": {"$sum": 1},  # 投注次数
                    # "total_bet_amount": {"$sum": "$bet_amount"},  # 投注额
                    "total_valid_bet_amount": {"$sum": "$valid_bet_amount"},  # 有效投注
                    "total_net_amount": {"$sum": "$net_amount"}  # 会员输赢
                }
            },
            {
                "$sort": {"_id.date": 1}  # 按日期排序
            }
        ]

        # 执行聚合查询
        result = collection.aggregate(pipeline)

        # 将结果添加到聚合结果列表中
        for doc in result:
            aggregation_results.append({
                # "date": doc["_id"]["date"],
                "member_id": doc["_id"]["member_id"],
                "game_type": doc["_id"]["game_type"],
                # "venue_name": doc["_id"]["venue_name"],
                # "game_name": doc["_id"]["game_name"],
                # "betting_count": doc["betting_count"],
                # "total_bet_amount": doc["total_bet_amount"],
                "total_valid_bet_amount": doc["total_valid_bet_amount"],
                "total_net_amount": doc["total_net_amount"]
            })

    # 将聚合结果转换为 pandas DataFrame
    type_data = pd.DataFrame(aggregation_results)
    # 7:捕鱼，6:电子, 5:棋牌, 3:真人, 1:体育, 4:彩票，2:电竞，
    print(type_data)
    # 将数据透视（pivot），并设置新列名
    # 定义游戏类型的对应关系
    game_type_mapping = {
        1: '体育会员输赢',
        2: '电竞会员输赢',
        3: '真人会员输赢',
        4: '彩票会员输赢',
        5: '棋牌会员输赢',
        6: '电子会员输赢',
        7: '捕鱼会员输赢'
    }

    # 将数据透视（pivot），并设置新列名
    type_data_pivot = type_data.pivot_table(index='member_id',
                                            columns='game_type',
                                            values='total_net_amount',
                                            aggfunc='sum').reset_index()

    # 重命名列
    type_data_pivot.columns = ['member_id'] + [game_type_mapping.get(col, col) for col in type_data_pivot.columns[1:]]

    type_data_valid_pivot = type_data.pivot_table(index='member_id',
                                                  columns='game_type',
                                                  values='total_valid_bet_amount',
                                                  aggfunc='sum').reset_index()

    game_type_mapping = {
        1: '体育有效投注',
        2: '电竞有效投注',
        3: '真人有效投注',
        4: '彩票有效投注',
        5: '棋牌有效投注',
        6: '电子有效投注',
        7: '捕鱼有效投注'
    }

    # 将数据透视（pivot），并设置新列名
    type_data_valid_pivot.columns = ['member_id'] + [game_type_mapping.get(col, col) for col in
                                                     type_data_valid_pivot.columns[1:]]

    # 合并数据
    member_info = pd.merge(member_info, type_data_pivot, on='member_id', how='left')
    member_info = pd.merge(member_info, type_data_valid_pivot, on='member_id', how='left')

    connection = pymysql.connect(
        host='18.178.159.230',
        port=3366,
        user='bigdata',
        password='uvb5SOSmLH8sCoSU',
        database='u1_1000'
    )

    query = f'SELECT member_id,member_credit_level FROM finance_1000.finance_member_level'

    member_credit_level = pd.read_sql_query(query, connection)

    member_credit_level.columns = ['member_id', '会员信用层级']

    connection.close()

    member_info = pd.merge(member_info, member_credit_level, on='member_id', how='left')
    print(member_info.columns)

    # 筛选站点ID字段的数据
    filtered_member_info = member_info[member_info['站点ID'] == 2000]

    # 填充 '会员信用层级' 字段中的空值为 '0_0'
    if '会员信用层级' in filtered_member_info.columns:
        filtered_member_info['会员信用层级'] = filtered_member_info['会员信用层级'].fillna('0_0')

    # type_data_pivot.to_csv('会员类型输赢统计.csv', index=False)
    print(filtered_member_info.head())
    filtered_member_info.to_excel(folder_path + '会员报表.xlsx', index=False)

# ----------------------------------------------------------------------------------------------------------------------
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
# BW01-黄金体育-数据对接群 群的 chat_id -1002362708863
CHAT_ID = '7523061850'

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
    # 创建两个任务，分别处理中午12点和晚上11:30的任务
    task_cunkuan_qukuan = asyncio.create_task(schedule_cunkuan_qukuan())
    task_wallet = asyncio.create_task(schedule_wallet())

    # 等待所有任务完成（主程序不会退出）
    await asyncio.gather(task_cunkuan_qukuan, task_wallet)


async def schedule_cunkuan_qukuan():
    while True:
        # 等待到中午12点
        await asyncio.sleep(get_time_until(11, 55))

        # 执行中午12点的任务
        export_data_from_database_cunkuan()
        export_data_from_database_qukuan()
        export_data_from_database_memberinfo()

        # 执行发送文件和删除文件的操作
        await send_files_in_folder(bot, folder_path, CHAT_ID)
        delete_all_files_in_directory(folder_path)


async def schedule_wallet():
    while True:
        # 等待到晚上11:30
        await asyncio.sleep(get_time_until(23, 30))

        # 执行晚上11:30的任务
        export_data_from_database_wallet()

        # 执行发送文件和删除文件的操作
        await send_files_in_folder(bot, folder_path, CHAT_ID)
        delete_all_files_in_directory(folder_path)


def get_time_until(target_hour, target_minute):
    now = datetime.datetime.now()

    # 设置下一个目标时间
    next_target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

    # 如果当前时间已经超过目标时间，则设为第二天的目标时间
    if now >= next_target_time:
        next_target_time += datetime.timedelta(days=1)

    # 计算时间差并返回秒数
    seconds_until_next_target = (next_target_time - now).total_seconds()
    print(f"下一次执行将在 {next_target_time.strftime('%Y-%m-%d %H:%M:%S')} 发生。")
    return seconds_until_next_target

# 启动异步任务
def run_bot():
    asyncio.run(main())


# 启动异步任务
if __name__ == "__main__":
    run_bot()
