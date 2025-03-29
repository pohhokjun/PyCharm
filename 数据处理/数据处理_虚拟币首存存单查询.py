import pymysql
import pandas as pd

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
      AND site_id = '1000'  
)
SELECT *
FROM ranked_records
WHERE rn = 1;
"""

df = pd.read_sql_query(query, connection)
print(df.columns)

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
print(df.columns)
df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '会员真实姓名', '会员等级', '上级ID', '账单号', '通用支付订单号',
              '订单金额', '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例',
              '支付状态', '客户端类型', '客户端IP', '充币名称', '收款人名称', '收款银行', '收款地址', '收款账号',
              '收款码', '收款银行名称', '支付结果', '确认时间', '管理员ID', '创建时间', '更新时间', '支付渠道',
              '支付渠道索引', '备注', '已支付金额', '已支付USDT金额', '订单状态', '转账账号', '充币账号', '财务备注',
              '支付时间', '操作员', '完成时间', '订单来源', '商户号', '数据路由', '支付渠道操作员', '协议', '预期USDT',
              '兑换USDT', 'APPID', '扩展字段1', '扩展字段2', '扩展字段3', '扩展字段8', '扩展字段11', '扩展字段12',
              '扩展字段13', '扩展字段15', '信用评级', '信用等级', '奖励比例', '奖励金额', '提醒', '请求来源', '设备号',
              '是否首充', '设备号成功次数', '客户端IP成功次数', '客户端IP位置', '客户端类型X', '排名']
df = df[['站点ID', '会员ID', '会员用户名', '会员等级', '上级ID', '账单号', '通用支付订单号', '订单金额',
         '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式', '流水比例', '支付状态', '流水比例',
         '支付状态', '支付结果', '确认时间', '操作员', '完成时间']]

# 输出数据到excel文件
df['支付方式'] = df['支付方式'].map({1003: '虚拟币扫码', 1018: 'EBPAY'})


def select(data, column, start_time, end_time):
    data = data[(data[column] >= start_time) & (data[column] < end_time)]
    return data

df = select(df, '确认时间', '2025-02-23', '2025-02-24')
df.to_excel('虚拟币首存单号.xlsx', index=False)
