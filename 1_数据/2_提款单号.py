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
            fw.site_id AS '站点ID',
            fw.member_id AS '会员ID',
            fw.member_username AS '会员用户名',
            fw.member_grade AS '会员等级',
            fw.top_id AS '上级ID',
            CASE fw.category
                WHEN 1 THEN '会员中心钱包提款'
                WHEN 3 THEN '代理钱包提款'
                WHEN 5 THEN '虚拟币钱包提款'
                WHEN 11 THEN '手动下分'
                WHEN 12 THEN '代客下分'
                ELSE fw.category
            END AS '类别',
            COALESCE(sv.dict_value, fw.withdraw_type) AS '提现类型',
            CASE fw.draw_status
                WHEN 101 THEN '发起，已扣款'
                WHEN 200 THEN '风控计算流水中'
                WHEN 201 THEN '自动风控不过，等待人工风控'
                WHEN 202 THEN '人工审核挂起'
                WHEN 300 THEN '已风控,待付款'
                WHEN 401 THEN '自动出款中'
                WHEN 402 THEN '已付款，提款成功'
                WHEN 403 THEN '已对账，提款成功'
                WHEN 500 THEN '已拒绝'
                WHEN 501 THEN '出款失败'
                ELSE fw.draw_status
            END AS '提现状态',
            fw.bill_no AS '账单号',
            fw.typay_order_id AS '通用支付订单号',
            fw.amount AS '订单金额',
            fw.usdt_amount AS '实际支付金额',
            fw.handling_fee AS '手续费',
            fw.created_at AS '创建时间',
            fw.confirm_at AS '确认时间',
            fw.payment_time AS '支付时间',
            fw.finance_remark AS '财务备注',
            fw.operator AS '操作员'
        FROM finance_1000.finance_withdraw fw
        LEFT JOIN (
            SELECT code, dict_value
            FROM (
                SELECT
                    code,
                    dict_value,
                    dict_code,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY code) AS rn
                FROM control_1000.sys_dict_value
                WHERE dict_code = 'withdraw_type'
            ) t
            WHERE rn = 1
        ) sv ON fw.withdraw_type = sv.code
        WHERE fw.draw_status IN (402, 403)
        AND fw.confirm_at BETWEEN '{start_time_str}' AND '{end_time_str}';
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
            file_name = f"【{site_name}】提款单号 {date_range_str}.xlsx"
            self.save_excel_with_freeze(site_df, os.path.join(self.save_path, file_name))

        print(f"所有站点提现数据已导出到 {self.save_path} 并冻结了首行")


def main():
    exporter = DataExporter()

    # 示例：使用昨天时间导出提现数据
    exporter.export_data(time_mode='yesterday')

    # 示例：使用指定时间导出提现数据
    # exporter.export_data(
    #     time_mode='manual',
    #     start_time='2025-04-01 00:00:00',
    #     end_time='2025-04-02 23:59:59'
    # )

if __name__ == "__main__":
    main()