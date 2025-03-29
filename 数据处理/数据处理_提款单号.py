import pymysql
import pandas as pd

connection = pymysql.connect(
    host='18.178.159.230',
    port=3366,
    user='bigdata',
    password='uvb5SOSmLH8sCoSU',
    database='finance_1000'
)

start_time = '2025-02-01 00:00:00'  # 开始时间
end_time = '2025-02-05 23:59:59'    # 结束时间
# site_id = '1000'  # 站点ID

# 使用f-string格式化查询语句
query = f"SELECT * FROM finance_1000.finance_withdraw WHERE draw_status IN (402, 403) AND confirm_at BETWEEN '{start_time}' AND '{end_time}';"


df = pd.read_sql_query(query, connection)
connection.close()
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
#        'reward_amount', 'transfer_member_id', 'transfer_member_name', 'risk_c_admin_id'],
#       dtype='object')

df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '手机号', '会员等级', '客户端类型', '客户端IP', '账单号', '通用支付订单号',
              '订单金额', '实际支付金额', '预存款', '预存款时间', '预存款成功次数', '银行代码', '银行卡号', '银行真实姓名',
              '类别', '提现状态', '风控管理员ID', '风控确认时间', '提现备注', '确认时间', '上级ID', '创建时间', '更新时间',
              '银行地址', '自动风控结果', '风控备注', '拒绝原因', '拒绝内容', '暂扣原因', '暂扣时间', '支付渠道',
              '提现类型', '支付渠道索引', '支付时间', '订单状态', '发货次数', '银行创建时间', '风控管理员姓名', '暂扣姓名',
              '会员真实姓名', '风控操作员', '财务备注', '商户号', '数据路由', '设备号', '风控接收时间', '协议',
              '兑换汇率', '预期币种', '手续费', '扩展字段0', '扩展字段1', '扩展字段2', '扩展字段3', '扩展字段4',
              '扩展字段8', '信用评级', '信用等级', '提醒', '请求来源', '系统类型', '系统类型信息',
              '支付组名称', '预提现', '操作员', '支付管理员ID', '奖励金额', '转账会员ID', '转账会员姓名', '风控C管理员ID']

df = df[['站点ID', '会员ID', '会员用户名', '会员等级', '账单号', '通用支付订单号',
              '订单金额', '实际支付金额', '预存款', '预存款时间', '预存款成功次数',
              '类别', '提现状态', '风控管理员ID', '风控确认时间', '提现备注', '确认时间', '上级ID', '创建时间', '更新时间',
              '自动风控结果', '风控备注', '拒绝原因', '拒绝内容', '暂扣原因', '暂扣时间', '支付渠道',
              '提现类型', '支付渠道索引', '支付时间', '订单状态', '发货次数', '银行创建时间', '风控管理员姓名',
              '风控操作员', '财务备注', '商户号', '设备号', '风控接收时间',
              '兑换汇率', '预期币种', '手续费',
              '信用评级', '信用等级', '提醒', '请求来源', '系统类型', '系统类型信息',
              '支付组名称', '预提现', '操作员', '支付管理员ID', '奖励金额', '转账会员ID', '转账会员姓名']]


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
    1002: "支付宝提款",
    0: "手动下分",
})

df[df['站点ID']==1000].to_excel('好博体育提款单号.xlsx', index=False)
df[df['站点ID']==2000].to_excel('黄金体育提款单号.xlsx', index=False)
df[df['站点ID']==7000].to_excel('蓝火体育提款单号.xlsx', index=False)
