import pymysql
import pandas as pd
from openpyxl import load_workbook
import os
from sqlalchemy import create_engine

# 时间范围（便于修改）
START_TIME = '2025-04-10 00:00:00'  # 开始时间
END_TIME = '2025-04-10 23:59:59'    # 结束时间

# 全局映射字典
PAY_METHODS = {
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
}

PAY_STATUS = {1: "发起", 2: "确认", 3: "已对账", 4: "用户关闭", 5: "订单失效(45分钟)"}
CATEGORIES = {0: "代客充值", 1: "会员存款", 2: "后台手动上分", 3: "佣金(代充)钱包转入充值", 6: "代理存款"}

SITES = {
    1000: '好博体育', 2000: '黄金体育', 3000: '宾利体育', 4000: 'HOME体育',
    5000: '亚洲之星', 6000: '玖博体育', 7000: '蓝火体育', 8000: 'A7体育'
}

SELECTED_COLUMNS = ['站点ID', '会员ID', '会员用户名', '会员等级', '上级ID', '账单号', '通用支付订单号',
                   '订单金额', '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别', '支付方式',
                   '流水比例', '支付状态', '支付结果', '创建时间', '确认时间', '操作员', '完成时间']

def save_excel_with_freeze(df, file_path):
    """保存Excel并冻结首行"""
    df.to_excel(file_path, index=False)
    wb = load_workbook(file_path)
    ws = wb.active
    ws.freeze_panes = 'A2'
    wb.save(file_path)

def main():
    # 数据库连接信息
    db_config = {
        'host': '18.178.159.230',
        'port': 3366,
        'user': 'bigdata',
        'password': 'uvb5SOSmLH8sCoSU',
        'database': 'finance_1000'
    }

    # 构建 SQLAlchemy 连接字符串
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

    # 查询数据
    query = f"SELECT * FROM finance_1000.finance_pay_records WHERE pay_status IN (2, 3) AND confirm_at BETWEEN '{START_TIME}' AND '{END_TIME}';"
    df = pd.read_sql_query(query, engine)

    # 重命名列
    df.columns = ['ID', '站点ID', '会员ID', '会员用户名', '会员真实姓名', '会员等级', '上级ID', '账单号',
                  '通用支付订单号', '订单金额', '实际支付金额', '支付流水号', '返利金额', '积分金额', '类别',
                  '支付方式', '流水比例', '支付状态', '客户端类型', '客户端IP', '充币名称', '收款人名称',
                  '收款银行', '收款地址', '收款账号', '收款码', '收款银行名称', '支付结果', '确认时间',
                  '管理员ID', '创建时间', '更新时间', '支付渠道', '支付渠道索引', '备注', '已支付金额',
                  '已支付USDT金额', '订单状态', '转账账号', '充币账号', '财务备注', '支付时间', '操作员',
                  '完成时间', '订单来源', '商户号', '数据路由', '支付渠道操作员', '协议', '预期USDT',
                  '兑换USDT', 'APPID', '扩展字段1', '扩展字段2', '扩展字段3', '扩展字段8', '扩展字段11',
                  '扩展字段12', '扩展字段13', '扩展字段15', '信用评级', '信用等级', '奖励比例', '奖励金额',
                  '提醒', '请求来源', '设备号', '是否首充', '设备号成功次数', '客户端IP成功次数', '客户端IP位置',
                  '客户端类型X']

    # 选择需要的列并应用映射
    df = df[SELECTED_COLUMNS].copy()  # 避免 SettingWithCopyWarning
    df.loc[:, '支付方式'] = df['支付方式'].map(PAY_METHODS)
    df.loc[:, '支付状态'] = df['支付状态'].map(PAY_STATUS)
    df.loc[:, '类别'] = df['类别'].map(CATEGORIES)

    # 保存路径
    save_path = r'C:\Henvita\0_数据导出'
    os.makedirs(save_path, exist_ok=True)

    # 格式化时间
    start_date_short = START_TIME[5:7] + '.' + START_TIME[8:10]
    end_date_short = END_TIME[5:7] + '.' + END_TIME[8:10]
    date_range_str = f"{start_date_short}-{end_date_short}"

    # 为每个站点生成文件
    for site_id, site_name in SITES.items():
        file_name = f"【{site_name}】存款单号 {date_range_str}.xlsx"
        file_path = os.path.join(save_path, file_name)
        site_df = df[df['站点ID'] == site_id].copy() # 避免 SettingWithCopyWarning
        save_excel_with_freeze(site_df, file_path)

    print(f"所有站点的数据已导出到 {save_path} 并冻结了首行")

if __name__ == "__main__":
    main()