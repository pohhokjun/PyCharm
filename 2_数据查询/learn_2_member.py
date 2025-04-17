from sqlalchemy import create_engine
import pymysql
import pandas as pd
import pymongo
from datetime import datetime
import zipfile
import io
import os

start_date = '2024-06-01'
end_date = '2025-04-05'
site_id = 1000

engine = create_engine("mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/u1_1000")
query = f"""
SELECT * FROM member_info
WHERE site_id = {site_id};
"""

member_info = pd.read_sql_query(query, engine)
print(member_info)

member_info.columns = ['member_id', '站点ID', '会员账号', '头像', '性别', '密码', '盐值', '状态',
                       '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称',
                       '标签编号', '最后登录IP', '注册设备ID', '最后登录设备ID',
                       '注册IP', '最后登录时间', '来源链接', '邀请码',
                       '邀请码来源', '注册设备', '登录设备', '地址加密',
                       '注册类型', '注册来源名称', '注册来源代码',
                       '应用ID', '手机号脱敏', '真实姓名脱敏',
                       '邮箱脱敏', '注册IP加密',
                       '注册IP脱敏', '最后登录IP加密',
                       '最后登录IP脱敏', '家庭电话脱敏', 'xs_s0',
                       'xs_s1', 'xs_s5', '标签名称', '标签', 'VIP等级', '登录认证',
                       '手机实名验证', 'SVIP', '省份加密', '注册时间',
                       '更新时间']

member_info = member_info[['member_id', '站点ID', '会员账号', '性别', '状态',
                           '代理ID', '上级代理', '是否代理', '生日', '地区代码', '昵称',
                           '标签编号', '最后登录时间', '来源链接', '注册设备', '登录设备',
                           'VIP等级', '登录认证', '手机实名验证', 'SVIP', '注册时间']]

member_info = member_info[['member_id', '站点ID', '会员账号', '状态', '上级代理', '标签编号', '最后登录时间', 'VIP等级', 'SVIP', '注册时间']]
member_info['状态'] = member_info['状态'].map({0: "禁用", 1: "启用"})

# ----------------------------------------------------------------------------------------------------------------------
# # 首存金额和首存时间
# engine = create_engine("mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/bigdata")
# query1 = f"""
# SELECT member_id,
#        ANY_VALUE(first_deposit_amount) AS 首存金额,
#        ANY_VALUE(first_deposit_time) AS  首存时间
# FROM (
#     SELECT *
#     FROM member_daily_statics
#     WHERE site_id = {site_id}
#     ORDER BY statics_date DESC
# ) AS sorted_table
# GROUP BY member_id
# """
# first_record = pd.read_sql_query(query1, engine)
# print(first_record.columns)
# print(first_record.head())
# member_info = pd.merge(member_info, first_record, on='member_id', how='outer')
# ----------------------------------------------------------------------------------------------------------------------
# 风控标签
engine = create_engine("mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/control_1000")
query = """
SELECT code,dict_value from sys_dict_value;
"""

dict_value = pd.read_sql_query(query, engine)
print(dict_value.columns)

dictionary = dict(zip(dict_value['code'], dict_value['dict_value']))

def replace_labels(label_string):
    labels = label_string.split(',')
    replaced_labels = [dictionary.get(label, label) for label in labels]
    # 过滤掉 162 这个标签
    filtered_labels = [label for label in replaced_labels if label != "162"]
    return ','.join(filtered_labels) if filtered_labels else ""  # 如果为空，则设为空字符串

member_info['风控标签'] = member_info['标签编号'].apply(replace_labels)
print(member_info.columns)
engine.dispose()

# ----------------------------------------------------------------------------------------------------------------------
# # 设置 MongoDB 连接
# client = pymongo.MongoClient("mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/")  # 根据实际连接信息修改
# db = client["update_records"]  # 切换到目标数据库
# start_time = start_date + " 00:00:00"
# end_time = end_date + " 23:59:59"
# # 获取所有以 "pull_order" 开头的 collection
# collections = [col for col in db.list_collection_names() if col.startswith('pull_order')]
#
# # 用于存储每个 member_id 的最后一条记录的列表
# last_records = []
#
# for collection_name in collections:
#     collection = db[collection_name]
#
#     # 聚合查询以获取每个 member_id 的最后一条记录
#     pipeline = [
#         {"$match": {"site_id": site_id}},
#         {"$sort": {"bet_time": -1}},  # 按 bet_time 降序排序
#         {"$group": {
#             "_id": "$member_id",
#             "last_record": {"$first": "$$ROOT"}  # 获取每个 member_id 的第一条记录，即最后一条记录
#         }},
#         {"$project": {"_id": 0, "member_id": 1, "record": "$last_record"}}  # 重新格式化输出
#     ]
#
#     # 执行聚合查询
#     results = collection.aggregate(pipeline)
#
#     for result in results:
#         last_records.append(result['record'])
#
# # 将数据转换为 DataFrame
# last_venue_data = pd.DataFrame(last_records)
#
# # 打印 DataFrame
# print(last_venue_data.columns)
#
# # 保留df中每个member_id关于bet_time的最后一条记录
# last_records = last_venue_data.sort_values(by=['member_id', 'bet_time'], ascending=False)
# last_records = last_records.drop_duplicates(subset=['member_id'], keep='first')
# # last_records = last_venue_data.groupby('member_id')['bet_time'].max().reset_index()
# last_records = last_records[['member_id', 'bet_time']]
# last_records.columns = ['member_id', '最后投注时间']
# client.close()
# ----------------------------------------------------------------------------------------------------------------------
# 合并数据
# member_info = pd.merge(member_info, last_records, on='member_id', how='left')
print(member_info.columns)
print(member_info.head())
member_info.to_excel("member_info.xlsx", index=False)