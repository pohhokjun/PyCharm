import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymongo
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
import os
import requests
import traceback
import re


def execute_mongo_aggregation(collection_name: str, pipeline: list, mongo_uri: str, db_name: str) -> pd.DataFrame:
   """执行 MongoDB 聚合查询的通用方法"""
   client = pymongo.MongoClient(mongo_uri, connectTimeoutMS=30000, serverSelectionTimeoutMS=30000)
   collection = client[db_name][collection_name]
   try:
       df = pd.DataFrame(list(collection.aggregate(pipeline, cursor={}, batchSize=5000)))
   except pymongo.errors.PyMongoError as e:
       print(f"MongoDB query failed for {collection_name}: {e}")
       return pd.DataFrame()
   finally:
       client.close()
   return df


class DatabaseQuery:
   def __init__(self, host: str, port: int, user: str, password: str,
                mongo_host: str, mongo_port: int, mongo_user: str, mongo_password: str,
                site_id: int = None, start_date: str = '2024-06-01', end_date: str = '2025-04-30',
                agent_1000: str = 'agent_1000', u1_1000: str = 'u1_1000',
                bigdata: str = 'bigdata', control_1000: str = 'control_1000',
                finance_1000: str = 'finance_1000',
                mongo_collection_prefix: str = 'pull_order_game_', venue: str = 'TY'):
       """初始化数据库连接参数"""
       # MySQL 连接
       connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{bigdata}"
       self.engine = create_engine(connection_string)
       self.Session = sessionmaker(bind=self.engine)
       self.session = self.Session()

       # MongoDB 连接参数
       self.mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/"
       self.mongo_db_name = "update_records"
       self.client = pymongo.MongoClient(self.mongo_uri, connectTimeoutMS=30000, serverSelectionTimeoutMS=30000)
       self.db = self.client[self.mongo_db_name]
       self.mongo_collection_prefix = mongo_collection_prefix
       self.venue = venue
       self.batch_size = 5000
       self.flag_value = 1

       # 其他参数
       self.control_1000 = control_1000
       self.bigdata = bigdata
       self.agent_1000 = agent_1000
       self.u1_1000 = u1_1000
       self.finance_1000 = finance_1000
       self.site_id = site_id
       self.start_date = start_date
       self.end_date = end_date
       self.start_time = f"{start_date} 00:00:00"
       self.end_time = f"{end_date} 23:59:59"

   def _process_mongo_collections(self, collections: list, pipeline: list) -> pd.DataFrame:
       """使用多进程处理 MongoDB 的通用方法"""
       processes = 15  # 固定使用 15 个进程
       with Pool(processes=processes) as pool:
           partial_process = partial(
               execute_mongo_aggregation,
               pipeline=pipeline,
               mongo_uri=self.mongo_uri,
               db_name=self.mongo_db_name
           )
           results = list(
               tqdm(pool.imap(partial_process, collections), total=len(collections), desc="Processing collections"))
       return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

   def close_connections(self):
       """关闭 MySQL 和 MongoDB 连接"""
       if self.session:
           self.session.close()
       self.client.close()

   def _1_member_basic_info(self) -> pd.DataFrame:
       """查询会员基本信息"""
       query = f"""
       SELECT
          u1_mi.site_id AS '站点ID',
          u1_mi.top_name AS '代理名称',
          u1_mi.id AS '会员ID',
          u1_mi.name AS '会员账号',
          CASE u1_mi.status WHEN 1 THEN '启用' WHEN 0 THEN '禁用' ELSE CAST(u1_mi.status AS CHAR) END AS '状态',
          u1_mi.vip_grade AS 'VIP等级',
          (SELECT GROUP_CONCAT(DISTINCT c1_sv.dict_value ORDER BY c1_sv.code SEPARATOR ',')
           FROM control_1000.sys_dict_value c1_sv
           WHERE FIND_IN_SET(c1_sv.code, u1_mi.tag_id)
           AND (c1_sv.initial_flag IS NULL OR c1_sv.initial_flag <> 1)) AS '标签'
       FROM u1_1000.member_info u1_mi
       LEFT JOIN (
          SELECT member_id, remark
          FROM (
              SELECT member_id, remark,
                     ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY updated_at DESC) AS rn
              FROM u1_1000.member_open_forbid_record
              WHERE remark_type = 1
          ) t
          WHERE t.rn = 1
       ) u1_mofr ON u1_mi.id = u1_mofr.member_id
       LEFT JOIN (
          SELECT DISTINCT member_info_id
          FROM u1_1000.member_banks_info
       ) u1_mbi ON u1_mi.id = u1_mbi.member_info_id
       """
       # 动态添加 WHERE 条件
       if self.site_id is not None:
           query += f" WHERE u1_mi.site_id = {self.site_id}"

       return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

   def _6_member_stats_period(self) -> pd.DataFrame:
       """查询会员指定时间段的按月份统计信息"""
       query = f"""
      SELECT
          DATE_FORMAT(statics_date, '%Y-%m') AS '月份',
          member_id AS '会员ID',
          COALESCE(SUM(deposit_count), 0) AS '存款笔数',
          COALESCE(SUM(deposit), 0) AS '存款金额',
          COALESCE(SUM(draw_count), 0) AS '取款笔数',
          COALESCE(SUM(draw), 0) AS '取款金额',
          COALESCE(SUM(promo), 0) AS '红利',
          COALESCE(SUM(rebate), 0) AS '返水'
      FROM {self.bigdata}.member_daily_statics
      WHERE statics_date BETWEEN '{self.start_date}' AND '{self.end_date}'
      GROUP BY member_id, DATE_FORMAT(statics_date, '%Y-%m')
      """
       return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)

   def mongo_betting_stats(self) -> pd.DataFrame:
       """查询 MongoDB 投注统计数据，强制按月份统计"""
       collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
       if not collections:
           return pd.DataFrame(columns=['月份', '会员ID', '投注次数', '有效投注', '会员输赢'])

       pipeline = [
           {"$match": {
               "flag": 1,
               "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
           }},
           {"$sort": {"settle_time": 1}},
           {"$group": {
               "_id": {
                   "member_id": "$member_id",
                   "game_type": "$game_type",
                   "date": {"$dateToString": {"format": "%Y-%m", "date": {"$toDate": "$settle_time"}}}
               },
               "betting_count": {"$sum": 1},
               "valid_bet": {"$sum": "$valid_bet_amount"},
               "net_amount": {"$sum": "$net_amount"}
           }},
           {"$project": {
               "_id": 0,
               "月份": "$_id.date",
               "会员ID": "$_id.member_id",
               "game_type": "$_id.game_type",
               "betting_count": 1,
               "valid_bet": 1,
               "net_amount": 1
           }}
       ]
       if self.site_id is not None:
           pipeline[0]["$match"]["site_id"] = self.site_id

       df = self._process_mongo_collections(collections, pipeline)
       if df.empty:
           return pd.DataFrame(columns=['月份', '会员ID', '投注次数', '有效投注', '会员输赢'])

       df = df.astype({'会员ID': 'category', 'game_type': 'int8', 'betting_count': 'int32',
                       'valid_bet': 'float32', 'net_amount': 'float32'})
       df['月份'] = df['月份'].astype('string')

       # 聚合会员统计
       member_stats = df.groupby(['月份', '会员ID'], observed=True).agg({
           'betting_count': 'sum',
           'valid_bet': 'sum',
           'net_amount': 'sum'
       }).reset_index().rename(columns={
           'betting_count': '投注次数',
           'valid_bet': '有效投注',
           'net_amount': '会员输赢'
       })

       # 游戏类型映射
       game_types = {
           1: ('体育有效投注', '体育会员输赢'),
           2: ('电竞有效投注', '电竞会员输赢'),
           3: ('真人有效投注', '真人会员输赢'),
           4: ('彩票有效投注', '彩票会员输赢'),
           5: ('棋牌有效投注', '棋牌会员输赢'),
           6: ('电子有效投注', '电子会员输赢'),
           7: ('捕鱼有效投注', '捕鱼会员输赢')
       }

       # 有效投注和输赢透视表
       valid_pivot = df.pivot_table(index=['月份', '会员ID'], columns='game_type', values='valid_bet',
                                    aggfunc='sum', fill_value=0, observed=False).reset_index()
       valid_pivot.columns = ['月份', '会员ID'] + [game_types.get(col, (str(col),))[0] for col in
                                                   valid_pivot.columns[2:]]
       net_pivot = df.pivot_table(index=['月份', '会员ID'], columns='game_type', values='net_amount',
                                  aggfunc='sum', fill_value=0, observed=False).reset_index()
       net_pivot.columns = ['月份', '会员ID'] + [game_types.get(col, (str(col),))[1] for col in
                                                 net_pivot.columns[2:]]

       # 修改排序：按场馆顺序合并有效投注和会员输赢
       result = member_stats
       for game_type in sorted(game_types.keys()):
           valid_col = game_types[game_type][0]
           net_col = game_types[game_type][1]
           if valid_col in valid_pivot.columns:
               result = result.merge(valid_pivot[['月份', '会员ID', valid_col]], on=['月份', '会员ID'], how='outer')
           if net_col in net_pivot.columns:
               result = result.merge(net_pivot[['月份', '会员ID', net_col]], on=['月份', '会员ID'], how='outer')

       # 确保最终列顺序
       final_columns = ['月份', '会员ID', '投注次数', '有效投注', '会员输赢'] + \
                       [col for col in result.columns if col not in ['月份', '会员ID', '投注次数', '有效投注', '会员输赢']]
       result = result.reindex(columns=final_columns)

       return result[result['有效投注'] > 0]

def save_to_excel(df: pd.DataFrame, filename: str):
   """保存 DataFrame 到 Excel 文件"""
   with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
       df.drop(columns=['member_id'], errors='ignore').to_excel(writer, sheet_name='Sheet1', index=False)
       workbook = writer.book
       worksheet = writer.sheets['Sheet1']
       # 添加百分比格式
       percent_format = workbook.add_format({'num_format': '0.00%'})
       for col_num, col_name in enumerate(df.columns):
           if '占比' in col_name:
               worksheet.set_column(col_num, col_num, None, percent_format)
       worksheet.freeze_panes(1, 0)
       worksheet.autofilter(0, 0, 0, len(df.columns) - 1)


def work(db_query: DatabaseQuery) -> pd.DataFrame:
   """执行查询并合并结果"""
   result_df = (db_query._1_member_basic_info()
                .merge(db_query._6_member_stats_period(), on='会员ID', how='left')
                .merge(db_query.mongo_betting_stats(), on=['会员ID', '月份'], how='left')
                )
   # 定义游戏类型字段
   game_bet_columns = [
       '体育有效投注', '电竞有效投注', '真人有效投注', '彩票有效投注',
       '棋牌有效投注', '电子有效投注', '捕鱼有效投注'
   ]

   # 计算每个游戏类型的占比
   for col in game_bet_columns:
       ratio_col = f"{col}占比"
       result_df[ratio_col] = result_df[col].fillna(0) / result_df['有效投注'].replace(0, 1)  # 避免除以0
       result_df[ratio_col] = result_df[ratio_col].fillna(0)  # 缺失值填0

   # 计算会员投注偏好：如果有效投注为0，显示“无”，否则取占比最高的游戏类型
   ratio_columns = [f"{col}占比" for col in game_bet_columns]
   result_df['会员投注偏好'] = result_df.apply(
       lambda row: '无' if row['有效投注'] == 0 or pd.isna(row['有效投注'])
       else row[ratio_columns].idxmax().replace('有效投注占比', ''),
       axis=1
   )

   return result_df


def main():
   start_time = datetime.now()
   print(f"运行开始时间: {start_time.strftime('%Y-%m-%d %H:%M')}")
   db_query = DatabaseQuery(
       host='18.178.159.230',
       port=3366,
       user='bigdata',
       password='uvb5SOSmLH8sCoSU',
       mongo_host='18.178.159.230',
       mongo_port=27217,
       mongo_user='biddata',
       mongo_password='uvb5SOSmLH8sCoSU'
   )
   try:
       result = work(db_query)
       if db_query.start_date == db_query.end_date:
           date_str = datetime.strptime(db_query.start_date, '%Y-%m-%d').strftime('%#m-%#d')
       else:
           date_str = f"{datetime.strptime(db_query.start_date, '%Y-%m-%d').strftime('%#m-%#d')}-{datetime.strptime(db_query.end_date, '%Y-%m-%d').strftime('%#m-%#d')}"
       excel_filename = f"【{db_query.site_id if db_query.site_id is not None else 'ALL'}_{date_str}_{db_query.venue}】{start_time.strftime('%#m-%#d_%H.%M')}.xlsx"
       save_to_excel(result, excel_filename)
       print(f"结果已保存到: {excel_filename}")
   except Exception as e:
       print(f"运行失败: {e}")
       traceback.print_exc()
   finally:
       db_query.close_connections()
       end_time = datetime.now()
       print(f"运行结束时间: {end_time.strftime('%Y-%m-%d %H:%M')}")
       print(f"总运行时间: {str(end_time - start_time).split('.')[0]}")


if __name__ == "__main__":
   main()
