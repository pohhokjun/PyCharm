import pandas as pd
from sqlalchemy import create_engine, text

# 数据库连接信息
db_config = {
    'host': '18.178.159.230',
    'port': 3366,
    'user': 'bigdata',
    'password': 'uvb5SOSmLH8sCoSU',
    'database': 'finance_1000'
}

# 构建 SQLAlchemy 连接字符串
engine_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# SQL 查询语句
query = """
WITH ranked_records AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY updated_at) AS rn
    FROM finance_1000.finance_pay_records
    WHERE pay_status IN (2, 3)
      AND pay_type IN (1003, 1018)
      AND site_id = '1000'
)
SELECT site_id AS '站点ID',
       member_id AS '会员ID',
       member_username AS '会员用户名',
       member_grade AS '会员等级',
       top_id AS '上级ID',
       bill_no AS '账单号',
       typay_order_id AS '通用支付订单号',
       order_amount AS '订单金额',
       pay_amount AS '实际支付金额',
       pay_seq AS '支付流水号',
       rebate_amount AS '返利金额',
       score_amount AS '积分金额',
       category AS '类别',
       CASE pay_type
           WHEN 1003 THEN '虚拟币扫码'
           WHEN 1018 THEN 'EBPAY'
           ELSE CAST(pay_type AS CHAR)
       END AS '支付方式',
       flow_ratio AS '流水比例',
       pay_status AS '支付状态',
       pay_result AS '支付结果',
       confirm_at AS '确认时间',
       operator AS '操作员',
       complete_time AS '完成时间'
FROM ranked_records
WHERE rn = 1
  AND confirm_at >= '2025-04-09'
  AND confirm_at < '2025-04-09';
"""

try:
    # 创建 SQLAlchemy 引擎
    engine = create_engine(engine_url)

    # 读取数据到 DataFrame
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection)

    # 如果 DataFrame 为空，提示用户
    if df.empty:
        print("没有符合条件的数据。")
    else:
        # 保存到 Excel 文件，冻结首行
        excel_path = r'C:\Henvita\0_数据导出\虚拟币首存单号.xlsx'
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # 写入数据，不写入默认的列名，从第二行开始写入数据
            df.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=1)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            worksheet.freeze_panes(1, 0)  # 冻结首行
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            # 手动写入列名到第一行
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            # 设置列宽
            for i, width in enumerate(df.apply(lambda x: max(len(str(v)) for v in x) + 2)):
                worksheet.set_column(i, i, width)

        print(f"数据已导出到：{excel_path}")

except Exception as e:
    print(f"发生错误：{e}")