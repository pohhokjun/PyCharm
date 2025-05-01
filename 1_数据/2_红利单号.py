import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os

# 记录开始时间
start_time = datetime.now()
print(f"运行开始时间 {start_time.strftime('%Y-%m-%d %H:%M')}")

# 数据库连接
engine = create_engine("mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/activity_1000")


# 定义SQL查询函数
def get_member_dividend_data(start_date, end_date, site_id):
    query = f"""
    SELECT 
        CAST(id AS CHAR) AS ID,  -- 确保 ID 列为字符串格式
        site_id AS 申请站点,
        bill_no AS 订单号,
        member_id AS 会员ID,
        member_name AS 会员账号,
        wallet_category AS 钱包类别,
        money AS 红利金额,
        CASE 
            WHEN status = 1 THEN '处理中'
            WHEN status = 2 THEN '成功'
            WHEN status = 3 THEN '失败'
            ELSE status
        END AS 状态,
        created_at AS 申请时间,
        updated_at AS 发放时间,
        check_user AS 审核用户,
        check_remark AS 审核备注,
        applicant AS 操作人,
        applicant_remark AS 申请备注,
        flow_times AS 流水倍数,
        activity_id AS 活动ID,
        activity_title AS 活动标题
    FROM activity_1000.member_dividend
    WHERE updated_at BETWEEN '{start_date}' AND '{end_date}'
    AND category NOT IN (999555)
    AND site_id = {site_id};
    """
    return query


# 参数
start_date = '2025-02-01 00:00:00'
end_date = '2025-04-30 23:59:59'
site_id = 7000

# 执行查询
query = get_member_dividend_data(start_date, end_date, site_id)
df = pd.read_sql_query(query, engine)

# 映射 site_id 到站点名称
site_mapping = {
    1000: "好博体育",
    2000: "黄金体育",
    3000: "宾利体育",
    4000: "HOME体育",
    5000: "亚洲之星",
    6000: "玖博体育",
    7000: "蓝火体育",
    8000: "A7体育",
    9000: "K9体育",
    9001: "摩根体育",
    9002: "友博体育"
}
df['申请站点'] = df['申请站点'].map(site_mapping)

# 获取当前脚本名称（不含扩展名）
script_name = os.path.splitext(os.path.basename(__file__))[0]

# 生成带时间戳的 Excel 文件名
current_time = datetime.now().strftime('%Y-%m-%d_%H.%M')
excel_filename = f"{script_name}_{current_time}.xlsx"

# 使用 openpyxl 引擎创建 Excel 文件
with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
    df.to_excel(writer, sheet_name='Sheet1', index=False)

    # 获取工作簿和工作表
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # 冻结首行
    worksheet.freeze_panes = 'A2'

    # 为所有列启用自动筛选
    worksheet.auto_filter.ref = worksheet.dimensions

print("excel自动冻结首行和设置好筛选功能")
print(f"excel名：{excel_filename}")

# 记录并打印结束时间
end_time = datetime.now()
print(f"运行结束时间 {end_time.strftime('%Y-%m-%d %H:%M')}")
