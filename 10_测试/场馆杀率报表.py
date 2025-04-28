import os
import pandas as pd
import pymongo
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyecharts.charts import Line, Page
from pyecharts import options as opts
from pyecharts.globals import ThemeType

# 参数配置
db_uri = "mongodb://biddata:uvb5SOSmLH8sCoSU@18.178.159.230:27217/"
db_name = "update_records"
start_date = '2024-06-01'
end_date   = '2025-04-27'

# 类型映射
game_type_map = {1: '体育', 2: '电竞', 3: '真人', 4: '彩票', 5: '棋牌', 6: '电子', 7: '捕鱼'}

def fetch_collection_data(coll_name):
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    collection = db[coll_name]
    start_time = f"{start_date} 00:00:00"
    end_time   = f"{end_date} 23:59:59"
    pipeline = [
        {"$match": {"flag": 1,
                    "settle_time": {"$gte": start_time, "$lte": end_time}}},
        {"$addFields": {"settle_time_date": {"$toDate": "$settle_time"}}},
        {"$group": {
            "_id": {"year": {"$year": "$settle_time_date"},
                    "month": {"$month": "$settle_time_date"}},
            "sum_valid_bet": {"$sum": "$valid_bet_amount"},
            "sum_net_amt":   {"$sum": "$net_amount"},
            "first_game_type": {"$first": "$game_type"}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    venue_name = coll_name.replace("pull_order_game_", "")
    rows = []
    for doc in results:
        y = doc['_id']['year']
        m = doc['_id']['month']
        date_str = f"{y}-{m:02d}"
        valid_bet = doc.get('sum_valid_bet', 0) or 0
        net_amt   = doc.get('sum_net_amt', 0) or 0
        company_win = -net_amt
        surplus_ratio = f"{(company_win/valid_bet*100):.2f}%" if valid_bet else '0.00%'
        venue_type = game_type_map.get(doc.get('first_game_type'), '未知')
        rows.append({
            '日期': date_str,
            '场馆名称': venue_name,
            '场馆类型': venue_type,
            '有效投注': valid_bet,
            '公司输赢': company_win,
            '盈余比例': surplus_ratio
        })
    client.close()
    return rows

# 并行采集所有集合
def collect_data():
    client = pymongo.MongoClient(db_uri)
    db = client[db_name]
    collections = [c for c in db.list_collection_names() if c.startswith('pull_order_game_')]
    client.close()

    all_rows = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_collection_data, col): col for col in collections}
        for fut in as_completed(futures):
            try:
                all_rows.extend(fut.result())
            except Exception as e:
                print(f"Error processing {futures[fut]}: {e}")
    return all_rows

# 构建 DataFrame
df = pd.DataFrame(collect_data())
# 保留有效行
df = df[df['日期'] != '1900-01']

# 按日期、场馆类型排序
df['日期_dt'] = pd.to_datetime(df['日期'], format="%Y-%m")
df.sort_values(['场馆类型', '日期_dt'], inplace=True)

# 准备 X 轴刻度
x_axis = sorted(df['日期'].unique())

# 使用 pyecharts Page 布局 7 个不同类型场馆的折线图
page = Page(layout=Page.SimplePageLayout)

for type_name in game_type_map.values():
    sub_df = df[df['场馆类型'] == type_name]
    if sub_df.empty:
        continue
    # 创建折线图
    line = (
        Line(init_opts=opts.InitOpts(width='600px', height='400px', theme=ThemeType.DARK))
        .add_xaxis(x_axis)
    )
    # 按场馆名称分组，多条折线
    for venue, group in sub_df.groupby('场馆名称'):
        # 获取对应日期点的盈余比例数值
        y_vals = [
            float(group[group['日期'] == d]['盈余比例'].str.rstrip('%').values[0])
            if not group[group['日期'] == d].empty else 0
            for d in x_axis
        ]
        line.add_yaxis(
            series_name=venue,
            y_axis=y_vals,
            is_smooth=True,
            label_opts=opts.LabelOpts(is_show=True, position='top')
        )
    # 全局配置
    line.set_global_opts(
        title_opts=opts.TitleOpts(title=f"{type_name}场馆 盈余比例趋势", subtitle=f"{start_date} ~ {end_date}"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        legend_opts=opts.LegendOpts(pos_top='5%', type_='scroll'),
        datazoom_opts=[opts.DataZoomOpts(type_='slider', range_start=0, range_end=100)]
    )
    page.add(line)

# 输出到 HTML 页面
os.makedirs('output', exist_ok=True)
page.render(path='output/dashboard.html')

# 导出 DataFrame
df.drop(columns=['日期_dt'], inplace=True)
df.to_excel('output/report_data.xlsx', index=False)

print("完成：已生成7个类型场馆的折线图大屏(html)和 report_data.xlsx")
