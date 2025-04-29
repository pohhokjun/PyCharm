
import pandas as pd
from openpyxl import load_workbook
import os
from sqlalchemy import create_engine

# 时间范围
START_TIME = '2025-04-28 00:00:00'
END_TIME = '2025-04-28 23:59:59'

def save_excel_with_freeze(df, file_path):
    """保存 Excel 并冻结首行"""
    df.to_excel(file_path, index=False, engine='openpyxl')
    wb = load_workbook(file_path)
    wb.active.freeze_panes = 'A2'
    wb.save(file_path)

def main():
    # 数据库连接
    db_config = {
        'host': '18.178.159.230', 'port': 3366,
        'user': 'bigdata', 'password': 'uvb5SOSmLH8sCoSU',
        'database': 'finance_1000'
    }
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

    # 保存路径
    save_path = r'C:\Henvita\0_数据导出'
    os.makedirs(save_path, exist_ok=True)

    # 格式化时间
    date_range_str = f"{START_TIME[5:7]}.{START_TIME[8:10]}-{END_TIME[5:7]}.{END_TIME[8:10]}"

    # SQL 查询
    query = """
    SELECT 
        f.site_id AS '站点ID', 
        f.member_id AS '会员ID', 
        f.member_username AS '会员用户名',
        f.member_grade AS '会员等级', 
        f.top_id AS '上级ID', 
        f.bill_no AS '账单号',
        f.typay_order_id AS '通用支付订单号', 
        f.order_amount AS '订单金额',
        f.pay_amount AS '实际支付金额', 
        f.pay_seq AS '支付流水号',
        f.rebate_amount AS '返利金额', 
        f.score_amount AS '积分金额',
        CASE f.category 
            WHEN 0 THEN '代客充值' 
            WHEN 1 THEN '会员存款' 
            WHEN 2 THEN '后台手动上分'
            WHEN 3 THEN '佣金(代充)钱包转入充值' 
            WHEN 6 THEN '代理存款' 
            ELSE f.category 
        END AS '类别',
        COALESCE(sv.dict_value, f.pay_type) AS '支付方式',
        f.flow_ratio AS '流水比例',
        CASE f.pay_status 
            WHEN 1 THEN '发起' 
            WHEN 2 THEN '确认' 
            WHEN 3 THEN '已对账' 
            WHEN 4 THEN '用户关闭' 
            WHEN 5 THEN '订单失效(45分钟)' 
            ELSE f.pay_status 
        END AS '支付状态',
        f.pay_result AS '支付结果', 
        f.created_at AS '创建时间',
        f.confirm_at AS '确认时间', 
        f.operator AS '操作员', 
        f.complete_time AS '完成时间'
    FROM finance_1000.finance_pay_records f
    LEFT JOIN (
        SELECT code, dict_value
        FROM (
            SELECT 
                code, 
                dict_value,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY code) AS rn
            FROM control_1000.sys_dict_value
            WHERE (initial_flag IS NULL OR initial_flag <> 1)
        ) t
        WHERE rn = 1
    ) sv ON f.pay_type = sv.code
    WHERE f.pay_status IN (2, 3)
    AND f.confirm_at BETWEEN %s AND %s
    AND f.site_id IN (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000)
    """

    # 查询所有站点数据
    df = pd.read_sql(query, engine, params=(START_TIME, END_TIME))

    # 为每个站点生成文件
    for site_id, site_name in {
        1000: '好博体育', 2000: '黄金体育', 3000: '宾利体育', 4000: 'HOME体育',
        5000: '亚洲之星', 6000: '玖博体育', 7000: '蓝火体育', 8000: 'A7体育'
    }.items():
        site_df = df[df['站点ID'] == site_id]
        file_name = f"【{site_name}】存款单号 {date_range_str}.xlsx"
        save_excel_with_freeze(site_df, os.path.join(save_path, file_name))

    print(f"所有站点的数据已导出到 {save_path} 并冻结了首行")

if __name__ == "__main__":
    main()
