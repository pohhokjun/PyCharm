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

    def query_base(self, start_time, end_time):
        """基础 SQL 查询"""
        return f"""
       SELECT 
           f.site_id AS '站点ID', 
           f.member_id AS '会员ID', 
           f.member_username AS '会员用户名',
           f.member_grade AS '会员等级', 
           f.top_id AS '上级ID', 
           CASE f.category 
               WHEN 0 THEN '代客充值' 
               WHEN 1 THEN '会员存款' 
               WHEN 2 THEN '后台手动上分'
               WHEN 3 THEN '佣金(代充)钱包转入充值' 
               WHEN 6 THEN '代理存款' 
               ELSE f.category 
           END AS '类别',
           COALESCE(sv.dict_value, f.pay_type) AS '支付方式',
           CASE f.pay_status 
               WHEN 1 THEN '发起' 
               WHEN 2 THEN '确认' 
               WHEN 3 THEN '已对账' 
               WHEN 4 THEN '用户关闭' 
               WHEN 5 THEN '订单失效(45分钟)' 
               ELSE f.pay_status 
           END AS '支付状态',
           f.order_status AS '订单状态',
           f.bill_no AS '账单编号 ',
           f.typay_order_id AS '支付订单ID', 
           f.order_amount AS '订单金额',
           f.pay_amount AS '实际支付金额', 
           f.rebate_amount AS '返利金额', 
           f.score_amount AS '积分金额',
           f.flow_ratio AS '流水比例',
           f.created_at AS '创建时间',
           f.confirm_at AS '确认时间',
           f.remark AS '备注',
           f.finance_remark AS '财务备注',
           f.operator AS '操作人',
           f.is_first_deposit AS '是否首次存款'
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
       AND f.confirm_at BETWEEN '{start_time}' AND '{end_time}'
       """

    def query_with_pay_type(self, start_time, end_time):
        """带 pay_type 限制的 SQL 查询"""
        return f"""
        SELECT 
            f.site_id AS '站点ID', 
            f.member_id AS '会员ID', 
            f.member_username AS '会员用户名',
            f.member_grade AS '会员等级', 
            f.top_id AS '上级ID', 
            CASE f.category 
                WHEN 0 THEN '代客充值' 
                WHEN 1 THEN '会员存款' 
                WHEN 2 THEN '后台手动上分'
                WHEN 3 THEN '佣金(代充)钱包转入充值' 
                WHEN 6 THEN '代理存款' 
                ELSE f.category 
            END AS '类别',
            COALESCE(sv.dict_value, f.pay_type) AS '支付方式',
            CASE f.pay_status 
                WHEN 1 THEN '发起' 
                WHEN 2 THEN '确认' 
                WHEN 3 THEN '已对账' 
                WHEN 4 THEN '用户关闭' 
                WHEN 5 THEN '订单失效(45分钟)' 
                ELSE f.pay_status 
            END AS '支付状态',
            f.order_status AS '订单状态',
            f.bill_no AS '账单编号 ',
            f.typay_order_id AS '支付订单ID', 
            f.order_amount AS '订单金额',
            f.pay_amount AS '实际支付金额', 
            f.rebate_amount AS '返利金额', 
            f.score_amount AS '积分金额',
            f.flow_ratio AS '流水比例',
            f.created_at AS '创建时间',
            f.confirm_at AS '确认时间',
            f.remark AS '备注',
            f.finance_remark AS '财务备注',
            f.operator AS '操作人',
            f.is_first_deposit AS '是否首次存款'
        FROM (
            SELECT 
                f.*,
                ROW_NUMBER() OVER (PARTITION BY f.member_id ORDER BY f.confirm_at ASC) AS rn
            FROM finance_1000.finance_pay_records f
            WHERE f.pay_status IN (2, 3)
            AND f.pay_type IN (1003, 1018)
        ) f
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
        WHERE f.rn = 1
        AND f.confirm_at BETWEEN '{start_time}' AND '{end_time}'
        """

    def export_data(self, time_mode='yesterday', start_time=None, end_time=None, query_type='base'):
        """导出数据，允许选择时间筛选方式和 SQL 查询类型"""
        # 选择时间筛选方式
        if time_mode == 'manual' and start_time and end_time:
            start_time, end_time = self.get_manual_time(start_time, end_time)
        else:
            start_time, end_time = self.get_yesterday_time()

        # 格式化时间
        date_range_str = self.format_date_range(start_time, end_time)

        # 选择 SQL 查询
        query = self.query_base(start_time, end_time) if query_type == 'base' else self.query_with_pay_type(start_time, end_time)

        # 查询数据
        df = pd.read_sql(query, self.engine)

        # 为每个站点生成文件
        for site_id, site_name in {
            1000: '好博体育', 2000: '黄金体育', 3000: '宾利体育', 4000: 'HOME体育',
            5000: '亚洲之星', 6000: '玖博体育', 7000: '蓝火体育', 8000: 'A7体育',
            9000: 'K9体育', 9001: '摩根体育', 9002: '友博体育'
        }.items():
            site_df = df[df['站点ID'] == site_id]
            file_name = f"【{site_name}】存款单号 {date_range_str}.xlsx"
            self.save_excel_with_freeze(site_df, os.path.join(self.save_path, file_name))

        print(f"所有站点的数据已导出到 {self.save_path} 并冻结了首行")


def main():
    exporter = DataExporter()

    # 示例：使用昨天时间和基础查询
    exporter.export_data(time_mode='yesterday', query_type='base')

    # 示例：使用手动时间和带 pay_type 限制的查询
    # exporter.export_data(
    #     time_mode='manual',
    #     start_time='2025-05-02 00:00:00',
    #     end_time='2025-05-02 23:59:59',
    #     query_type='pay_type'
    # )


if __name__ == "__main__":
    main()
