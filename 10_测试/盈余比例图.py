
import os
import pandas as pd
from pyecharts.charts import Line, Pie, Page
from pyecharts import options as opts
from pyecharts.globals import ThemeType

# 配置
file_path = r'C:\Users\Administrator\Desktop\PyCharm\10_测试\output\report_data.xlsx'
game_type_map = {1: '体育', 2: '电竞', 3: '真人', 4: '彩票', 5: '棋牌', 6: '电子', 7: '捕鱼'}

def read_data(path):
    """读取并预处理 Excel 数据"""
    try:
        df = pd.read_excel(path, engine='openpyxl')
        # 过滤不需要的月份
        df = df[~df['日期'].isin(['2024-06', '2024-07', '2024-08'])]
        # 转换为 datetime，处理可能的格式错误
        df['日期_dt'] = pd.to_datetime(df['日期'], format="%Y-%m", errors='coerce')
        # 移除无效日期
        df = df.dropna(subset=['日期_dt'])
        # 按日期排序
        df = df.sort_values('日期_dt')
        return df
    except Exception as e:
        print(f"读取数据失败: {e}")
        return pd.DataFrame()

def create_bet_pie_chart(df, game_type_map):
    """生成按场馆类型统计有效投注的饼状图"""
    if df.empty or '有效投注' not in df.columns:
        print("数据为空或缺少'有效投注'列，无法生成饼状图")
        return None
    # 按场馆类型聚合有效投注
    pie_data = df.groupby('场馆类型')['有效投注'].sum().reset_index()
    # 过滤无效数据并转换为 game_type_map 中的名称
    pie_data = pie_data[pie_data['场馆类型'].isin(game_type_map.values())]
    if pie_data.empty:
        print("没有有效的场馆类型数据，无法生成饼状图")
        return None
    # 准备饼状图数据
    data_pair = [(row['场馆类型'], float(row['有效投注'])) for _, row in pie_data.iterrows()]
    pie = (
        Pie(init_opts=opts.InitOpts(width='628px', height='298px', theme=ThemeType.DARK))
        .add(
            "",
            data_pair,
            radius=["30%", "75%"],  # 环形图
            center=["50%", "50%"],
            label_opts=opts.LabelOpts(formatter="{b}: {d}%")
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="有效投注按场馆类型分布"),
            legend_opts=opts.LegendOpts(pos_top='5%', type_='scroll'),
        )
    )
    return pie

def create_pie_chart(df, game_type_map):
    """生成按场馆类型统计公司输赢的饼状图"""
    if df.empty or '公司输赢' not in df.columns:
        print("数据为空或缺少'公司输赢'列，无法生成饼状图")
        return None
    # 按场馆类型聚合公司输赢
    pie_data = df.groupby('场馆类型')['公司输赢'].sum().reset_index()
    # 过滤无效数据并转换为 game_type_map 中的名称
    pie_data = pie_data[pie_data['场馆类型'].isin(game_type_map.values())]
    if pie_data.empty:
        print("没有有效的场馆类型数据，无法生成饼状图")
        return None
    # 准备饼状图数据
    data_pair = [(row['场馆类型'], float(row['公司输赢'])) for _, row in pie_data.iterrows()]
    pie = (
        Pie(init_opts=opts.InitOpts(width='628px', height='298px', theme=ThemeType.DARK))
        .add(
            "",
            data_pair,
            radius=["30%", "75%"],  # 环形图
            center=["50%", "50%"],
            label_opts=opts.LabelOpts(formatter="{b}: {d}%")
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="公司输赢按场馆类型分布"),
            legend_opts=opts.LegendOpts(pos_top='5%', type_='scroll'),
        )
    )
    return pie

def create_line_chart(sub_df, x_axis, type_name, dates):
    """生成单类型场馆的折线图"""
    if sub_df.empty:
        return None
    line = (
        Line(init_opts=opts.InitOpts(width='628px', height='298px', theme=ThemeType.DARK))
        .add_xaxis(x_axis)
    )
    for venue, group in sub_df.groupby('场馆名称'):
        y_vals = []
        for d in dates:
            try:
                val = group[group['日期'] == d]['盈余比例'].iloc[0]
                # 移除 % 号并转换为浮点数
                y_vals.append(float(val.rstrip('%')) if isinstance(val, str) else float(val))
            except (IndexError, ValueError):
                y_vals.append(0)  # 数据缺失或格式错误时填充 0
        line.add_yaxis(
            series_name=venue,
            y_axis=y_vals,
            is_smooth=True,
            label_opts=opts.LabelOpts(is_show=True, position='top')
        )
    line.set_global_opts(
        title_opts=opts.TitleOpts(title=f"{type_name}场馆 盈余比例趋势"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        legend_opts=opts.LegendOpts(pos_top='5%', type_='scroll'),
        datazoom_opts=[opts.DataZoomOpts(type_='slider', range_start=0, range_end=100)]
    )
    return line

def generate_dashboard(df, game_type_map):
    """生成仪表板，包含折线图和饼状图"""
    if df.empty:
        print("数据为空，无法生成仪表板")
        return
    # X 轴显示年月
    x_axis = [pd.to_datetime(d).strftime('%m') for d in sorted(df['日期'].unique())]
    dates = sorted(df['日期'].unique())
    page = Page(layout=Page.SimplePageLayout)

    # 添加有效投注饼状图
    bet_pie = create_bet_pie_chart(df, game_type_map)
    if bet_pie:
        page.add(bet_pie)

    # 添加公司输赢饼状图
    pie = create_pie_chart(df, game_type_map)
    if pie:
        page.add(pie)

    # 添加折线图
    for type_name in game_type_map.values():
        sub_df = df[df['场馆类型'] == type_name]
        line = create_line_chart(sub_df, x_axis, type_name, dates)
        if line:
            page.add(line)

    os.makedirs('output', exist_ok=True)
    output_path = 'output/dashboard.html'
    page.render(path=output_path)
    print(f"完成：已生成仪表板（包含折线图和饼状图），保存至 {output_path}")

# 执行
df = read_data(file_path)
generate_dashboard(df, game_type_map)

