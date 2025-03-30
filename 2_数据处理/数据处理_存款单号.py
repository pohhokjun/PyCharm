import pymysql
import pandas as pd

connection = pymysql.connect(
    host='18.178.159.230',
    port=3366,
    user='bigdata',
    password='uvb5SOSmLH8sCoSU',
    database='finance_1000'
)

start_time = '2025-02-09 00:00:00'  # 开始时间
end_time = '2025-02-09 23:59:59'    # 结束时间
# site_id = '2000'  # 站点ID

# 使用f-string格式化查询语句
query = f"SELECT * FROM finance_1000.finance_pay_records WHERE pay_status IN (2, 3) AND confirm_at BETWEEN '{start_time}' AND '{end_time}';"


df = pd.read_sql_query(query, connection)
connection.close()
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

df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '会员真实姓名', '会员等级', '上级ID', '账单号', '通用支付订单号',
           '订单金额', '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例',
           '支付状态', '客户端类型', '客户端IP', '充币名称', '收款人名称', '收款银行', '收款地址', '收款账号',
           '收款码', '收款银行名称', '支付结果', '确认时间', '管理员ID', '创建时间', '更新时间', '支付渠道',
           '支付渠道索引', '备注', '已支付金额', '已支付USDT金额', '订单状态', '转账账号', '充币账号', '财务备注',
           '支付时间', '操作员', '完成时间', '订单来源', '商户号', '数据路由', '支付渠道操作员', '协议', '预期USDT',
           '兑换USDT', 'APPID', '扩展字段1', '扩展字段2', '扩展字段3', '扩展字段8', '扩展字段11', '扩展字段12',
           '扩展字段13', '扩展字段15', '信用评级', '信用等级', '奖励比例', '奖励金额', '提醒', '请求来源', '设备号',
           '是否首充', '设备号成功次数', '客户端IP成功次数', '客户端IP位置', '客户端类型X']

df = df[['站点ID', '会员ID', '会员用户名', '会员等级', '上级ID', '账单号', '通用支付订单号', '订单金额',
         '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例',
         '支付状态', '支付结果', '创建时间','确认时间', '操作员', '完成时间']]

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
df['类别'] = df['类别'].map({0: "代客充值", 1: "会员存款", 2: "后台手动上分", 3: "佣金(代充)钱包转入充值", 6: "代理存款"})

df[df['站点ID']==1000].to_excel('好博体育存款单号.xlsx', index=False)
df[df['站点ID']==2000].to_excel('黄金体育存款单号.xlsx', index=False)
df[df['站点ID']==4000].to_excel('HOME体育存款单号.xlsx', index=False)
df[df['站点ID']==7000].to_excel('蓝火体育存款单号.xlsx', index=False)
