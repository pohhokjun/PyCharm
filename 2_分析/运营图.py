import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# 设置 Plotly 渲染为 HTML
pio.renderers.default = 'browser'

# 读取 Excel 文件
file_path = '好博体育指标数据.xlsx'  # 请替换为实际文件路径
try:
    df = pd.read_excel(file_path, sheet_name='基本数据')
except FileNotFoundError:
    print("文件未找到，请检查路径！")
    exit(1)
except Exception as e:
    print(f"读取 Excel 文件时出错：{e}")
    exit(1)

# 打印列名以便调试
print("数据列名：", df.columns.tolist())

# 确保日期列为日期格式
if '日期' in df.columns:
    df['日期'] = pd.to_datetime(df['日期'])
else:
    print("未找到 '日期' 列，请检查数据！")
    exit(1)

# 创建 HTML 文件
html_content = """
<html>
<head>
    <title>GD - 数据指标概况</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #000; color: #fff; }
        h1 { color: #ddd; text-align: center; }
        h2 { color: #bbb; }
        .header { background-color: #222; padding: 10px; border-radius: 5px; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; background-color: #333; color: #fff; }
        th, td { border: 1px solid #555; padding: 8px; text-align: center; }
        th { background-color: #444; }
        .plot-container { margin-bottom: 40px; background-color: #333; padding: 10px; border-radius: 5px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>GD - 数据指标概况 - 数据部</h1>
        <p style="text-align: center; color: #ccc;">统计时间：04/13/25</p>
    </div>
"""

# 图表 1：总体统计表格
required_cols = ['注册数', '首存人数', '投注人数', '投注额', '有效投注额', '公司输赢含提前结算', '公司净收入']
available_cols = [col for col in required_cols if col in df.columns]
if not available_cols:
    print("未找到任何所需的统计列，请检查数据！")
    exit(1)

html_content += "<h2>总体统计</h2>\n"
latest_data = df[df['日期'] == df['日期'].max()][available_cols].iloc[0]
html_content += "<table>\n<tr>" + "".join(f"<th>{col}</th>" for col in available_cols) + "</tr>\n"
html_content += "<tr>" + "".join(f"<td>{latest_data[col]:,.2f}</td>" if not pd.isna(latest_data[col]) else "<td>-</td>" for col in available_cols) + "</tr>\n"
html_content += "</table>\n"

# 图表 2：注册数和首存人数（柱状图 + 折线图）
if '注册数' in df.columns and '首存人数' in df.columns:
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Bar(x=df['日期'], y=df['注册数'], name='注册数', marker_color='skyblue'), secondary_y=False)
    fig1.add_trace(go.Bar(x=df['日期'], y=df['首存人数'], name='首存人数', marker_color='lightblue'), secondary_y=False)
    fig1.add_trace(go.Scatter(x=df['日期'], y=df['注册数'], name='注册数趋势', line=dict(color='blue')), secondary_y=True)
    fig1.add_trace(go.Scatter(x=df['日期'], y=df['首存人数'], name='首存人数趋势', line=dict(color='darkblue')), secondary_y=True)
    fig1.update_layout(title='注册数与首存人数', height=400, width=800, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='white')
    fig1.update_yaxes(title_text="人数", secondary_y=False, color='white')
    fig1.update_yaxes(title_text="趋势", secondary_y=True, color='white')
    fig1.update_xaxes(color='white')
    html_content += '<div class="plot-container">\n' + fig1.to_html(full_html=False, include_plotlyjs=False) + '\n</div>\n'
else:
    print("缺少 '注册数' 或 '首存人数' 列，跳过图表 2")

# 图表 3：公司输赢含提前结算和公司净收入（柱状图）
if '公司输赢含提前结算' in df.columns and '公司净收入' in df.columns:
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=df['日期'], y=df['公司输赢含提前结算'], name='公司输赢含提前结算', marker_color='salmon'))
    fig2.add_trace(go.Bar(x=df['日期'], y=df['公司净收入'], name='公司净收入', marker_color='lightcoral'))
    fig2.update_layout(title='公司输赢与净收入', height=400, width=800, barmode='group', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='white')
    fig2.update_yaxes(color='white')
    fig2.update_xaxes(color='white')
    html_content += '<div class="plot-container">\n' + fig2.to_html(full_html=False, include_plotlyjs=False) + '\n</div>\n'
else:
    print("缺少 '公司输赢含提前结算' 或 '公司净收入' 列，跳过图表 3")

# 图表 4：存款额（柱状图）
if '存款额' in df.columns:
    fig3 = go.Figure()
    fig3.add_trace(go.Bar(x=df['日期'], y=df['存款额'], name='存款额', marker_color='gold'))
    fig3.update_layout(title='存款额', height=400, width=800, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='white')
    fig3.update_yaxes(color='white')
    fig3.update_xaxes(color='white')
    html_content += '<div class="plot-container">\n' + fig3.to_html(full_html=False, include_plotlyjs=False) + '\n</div>\n'
else:
    print("缺少 '存款额' 列，跳过图表 4")

# 图表 5：有效投注额（柱状图）
if '有效投注额' in df.columns:
    fig4 = go.Figure()
    fig4.add_trace(go.Bar(x=df['日期'], y=df['有效投注额'], name='有效投注额', marker_color='purple'))
    fig4.update_layout(title='有效投注额', height=400, width=800, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='white')
    fig4.update_yaxes(color='white')
    fig4.update_xaxes(color='white')
    html_content += '<div class="plot-container">\n' + fig4.to_html(full_html=False, include_plotlyjs=False) + '\n</div>\n'
else:
    print("缺少 '有效投注额' 列，跳过图表 5")

# 图表 6：红利和返水（折线图）
if '红利' in df.columns and '返水' in df.columns:
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=df['日期'], y=df['红利'], name='红利', line=dict(color='pink')))
    fig5.add_trace(go.Scatter(x=df['日期'], y=df['返水'], name='返水', line=dict(color='lightblue')))
    fig5.update_layout(title='红利与返水', height=400, width=800, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='white')
    fig5.update_yaxes(color='white')
    fig5.update_xaxes(color='white')
    html_content += '<div class="plot-container">\n' + fig5.to_html(full_html=False, include_plotlyjs=False) + '\n</div>\n'
else:
    print("缺少 '红利' 或 '返水' 列，跳过图表 6")

# 图表 7：存款人数（折线图 + 面积图）
if '存款人数' in df.columns:
    fig6 = go.Figure()
    fig6.add_trace(go.Scatter(x=df['日期'], y=df['存款人数'], name='存款人数', fill='tozeroy', line=dict(color='orange')))
    fig6.update_layout(title='存款人数', height=400, width=800, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='white')
    fig6.update_yaxes(color='white')
    fig6.update_xaxes(color='white')
    html_content += '<div class="plot-container">\n' + fig6.to_html(full_html=False, include_plotlyjs=False) + '\n</div>\n'
else:
    print("缺少 '存款人数' 列，跳过图表 7")

# 图表 8：公司净收入（柱状图）
if '公司净收入' in df.columns:
    fig7 = go.Figure()
    fig7.add_trace(go.Bar(x=df['日期'], y=df['公司净收入'], name='公司净收入', marker_color='lightcoral'))
    fig7.update_layout(title='公司净收入', height=400, width=800, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='white')
    fig7.update_yaxes(color='white')
    fig7.update_xaxes(color='white')
    html_content += '<div class="plot-container">\n' + fig7.to_html(full_html=False, include_plotlyjs=False) + '\n</div>\n'
else:
    print("缺少 '公司净收入' 列，跳过图表 8")

# 结束 HTML
html_content += "</body>\n</html>"

# 保存 HTML 文件
try:
    with open('dashboard.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    print("仪表板 HTML 文件已生成：dashboard.html")
except Exception as e:
    print(f"保存 HTML 文件时出错：{e}")