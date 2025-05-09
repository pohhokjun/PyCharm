import pandas as pd
from openpyxl import load_workbook
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta


class DataExporter:
    def __init__(self):
        self.db_config = {
            'host': '18.178.159.230', 'port': 3366,
            'user': 'bigdata', 'password': 'uvb5SOSmLH8sCoSU',
            'database': 'finance_1000'
        }
        self.save_path = r'C:\Henvita\0_数据导出'
        self.engine = create_engine(
            f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        os.makedirs(self.save_path, exist_ok=True)

    def save_excel_with_freeze(self, df, file_path):
        """保存 Excel 并冻结首行"""
        df.to_excel(file_path, index=False, engine='openpyxl')
        wb = load_workbook(file_path)
        wb.active.freeze_panes = 'A2'
        wb.active.auto_filter.ref = wb.active.dimensions
        wb.save(file_path)

    def format_date_range(self, start, end):
        """格式化时间范围为文件名字符串"""
        if start is None or end is None:
            return "所有时间"
        start_dt = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        end_dt = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        return f"{start_dt.strftime('%m.%d')}-{end_dt.strftime('%m.%d')}"

    def get_manual_time(self, start_time, end_time):
        """手动输入时间范围"""
        return start_time, end_time

    def get_yesterday_time(self):
        """自动获取昨天时间范围"""
        yesterday = datetime.now() - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999).strftime('%Y-%m-%d %H:%M:%S')
        return start, end

    def query_base(self, start_time_str, end_time_str):
        """使用提供的 SQL 导出提现数据"""
        query = f"""
        SELECT 
            a1_md.site_id AS 站点ID,
            a1_md.activity_title AS 活动标题,
            a1_oci.title AS 标题,
            a1_md.member_id AS 会员ID,
            a1_md.member_name AS 会员账号,
            a1_md.member_grade AS 会员等级,
            a1_md.bill_no AS 订单号,
            CASE 
                WHEN a1_md.wallet_category = 1 THEN '中心钱包'
                WHEN a1_md.wallet_category = 2 THEN '场馆钱包'
                ELSE a1_md.wallet_category
            END AS 钱包类别,
            a1_md.flow_times AS 流水倍数,
            a1_md.money AS 红利金额,
            CASE 
                WHEN a1_md.status = 1 THEN '处理中'
                WHEN a1_md.status = 2 THEN '成功'
                WHEN a1_md.status = 3 THEN '失败'
                ELSE a1_md.status
            END AS 状态,
            CASE 
                WHEN a1_md.issue_type = 1 THEN '手动发放'
                WHEN a1_md.issue_type = 2 THEN '自动发放'
                ELSE a1_md.issue_type
            END AS 发行类型,
            COALESCE(sv.dict_value, a1_md.activity_type) AS '活动类型',
            a1_md.created_at AS 申请时间,
            a1_md.applicant_remark AS 申请备注,
            a1_md.updated_at AS 发放时间,
            a1_md.check_user AS 审核用户,
            a1_md.check_remark AS 审核备注,
            a1_md.applicant AS 操作人
        FROM activity_1000.member_dividend a1_md
        LEFT JOIN activity_1000.operation_activity_info a1_oci 
            ON a1_md.activity_id = a1_oci.id
        LEFT JOIN (
            SELECT code, dict_value
            FROM (
                SELECT
                    code,
                    dict_value,
                    dict_code,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY code) AS rn
                FROM control_1000.sys_dict_value
                WHERE dict_code IN ('activity_type')
            ) t
            WHERE rn = 1
        ) sv ON a1_md.activity_type = sv.code
        WHERE a1_md.updated_at BETWEEN '{start_time_str}' AND '{end_time_str}'
        AND a1_md.category NOT IN (999555)
        """
        return query

    def export_data(self, time_mode='yesterday', start_time=None, end_time=None):
        """导出提现数据，允许选择时间筛选方式"""
        # 选择时间筛选方式
        if time_mode == 'manual' and start_time and end_time:
            start_time, end_time = self.get_manual_time(start_time, end_time)
        else:
            start_time, end_time = self.get_yesterday_time()

        date_range_str = self.format_date_range(start_time, end_time)
        query = self.query_base(start_time, end_time)
        df = pd.read_sql(query, self.engine)

        # 为每个站点生成文件
        for site_id, site_name in {
            1000: '好博体育', 2000: '黄金体育', 3000: '宾利体育', 4000: 'HOME体育',
            5000: '亚洲之星', 6000: '玖博体育', 7000: '蓝火体育', 8000: 'A7体育',
            9000: 'K9体育', 9001: '摩根体育', 9002: '友博体育'
        }.items():
            site_df = df[df['站点ID'] == site_id]
            file_name = f"【{site_name}】红利单号 {date_range_str}.xlsx"
            self.save_excel_with_freeze(site_df, os.path.join(self.save_path, file_name))

        print(f"所有站点提现数据已导出到 {self.save_path} 并冻结了首行")


def main():
    exporter = DataExporter()

    # 示例：使用昨天时间导出提现数据
    # exporter.export_data(time_mode='yesterday')

    # 示例：使用指定时间导出提现数据
    exporter.export_data(
        time_mode='manual',
        start_time='2025-04-01 00:00:00',
        end_time='2025-04-30 23:59:59'
    )

if __name__ == "__main__":
    main()