import pandas as pd
import os
import json
from pandas import Timestamp


# 自定义 JSON 编码器
class TimestampEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Timestamp):
            return obj.isoformat()
        return super().default(obj)


def time_to_seconds(time_str):
    """将时间字符串转换为秒数"""
    try:
        if isinstance(time_str, str):
            parts = time_str.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = map(float, parts)
                return hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 2:
                minutes, seconds = map(float, parts)
                return minutes * 60 + seconds
            elif len(parts) == 1:
                return float(parts[0])
        return 0
    except (ValueError, TypeError):
        return 0


def load_all_sheets(file_path):
    """加载所有 sheet 数据"""
    try:
        if not os.path.exists(file_path):
            print(f"错误：文件 {file_path} 不存在")
            return None
        excel_file = pd.ExcelFile(file_path)
        print(f"找到的 sheet 名称：{excel_file.sheet_names}")
        sheets_data = {}
        for sheet in excel_file.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet)
            if len(df.columns) < 3:
                print(f"警告：Sheet {sheet} 的列数不足，跳过处理")
                continue
            sheets_data[sheet] = df
        if not sheets_data:
            print("错误：没有有效的 sheet 数据")
            return None
        return sheets_data
    except ValueError as ve:
        print(f"错误：Excel 文件格式不正确 - {ve}")
        return None
    except Exception as e:
        print(f"错误：读取 Excel 文件时发生问题 - {e}")
        return None


def generate_excel_pivot_view_html():
    sheets_data = load_all_sheets("31日报表.xlsx")
    if sheets_data is None:
        return "<h1>错误：数据加载失败</h1>"

    sheets = list(sheets_data.keys())
    print(f"加载的 sheet：{sheets}")
    default_sheet = sheets[0] if sheets else ""
    colors = [
        ('rgba(54, 162, 235, 0.6)', 'rgba(54, 162, 235, 1)'),
        ('rgba(255, 99, 132, 0.6)', 'rgba(255, 99, 132, 1)'),
        ('rgba(255, 206, 86, 0.6)', 'rgba(255, 206, 86, 1)'),
        ('rgba(75, 192, 192, 0.6)', 'rgba(75, 192, 192, 1)'),
        ('rgba(153, 102, 255, 0.6)', 'rgba(153, 102, 255, 1)')
    ]

    all_data = {}
    for sheet in sheets:
        sheet_df = sheets_data[sheet]
        # X轴定义
        if sheet == "存款":
            x_axis_col = "存款类型"
        elif sheet == "取款":
            x_axis_col = "取款类型"
        else:
            x_axis_col = "日期"
        if x_axis_col not in sheet_df.columns:
            print(f"错误：Sheet {sheet} 未找到 '{x_axis_col}' 列")
            continue
        sheet_date_labels = sheet_df[x_axis_col].dropna().unique().tolist()
        value_columns = [col for col in sheet_df.columns if pd.api.types.is_numeric_dtype(sheet_df[col])]
        sheet_datasets = []

        if sheet == "人数":
            y_left_cols = ["注册人数", "首存人数"]
            y_right_cols = ["充值人数", "取款人数", "投注人数"]
            for i, column in enumerate(y_left_cols):
                if column in value_columns:
                    data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                    color_idx = i % len(colors)
                    sheet_datasets.append({
                        'label': column,
                        'data': data,
                        'backgroundColor': colors[color_idx][0],
                        'borderColor': colors[color_idx][1],
                        'borderWidth': 1,
                        'type': 'bar',
                        'yAxisID': 'y-left'
                    })
            for i, column in enumerate(y_right_cols):
                if column in value_columns:
                    data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                    color_idx = (i + len(y_left_cols)) % len(colors)
                    sheet_datasets.append({
                        'label': column,
                        'data': data,
                        'backgroundColor': colors[color_idx][0],
                        'borderColor': colors[color_idx][1],
                        'borderWidth': 2,
                        'type': 'line',
                        'yAxisID': 'y-right',
                        'fill': False
                    })
        elif sheet == "金额":
            y_left_cols = ["存提差", "公司输赢", "公司净收入", "代理佣金"]
            y_right_cols = ["提前结算", "账户调整", "红利", "返水", "集团分成"]
            for i, column in enumerate(y_left_cols):
                if column in value_columns:
                    data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                    color_idx = i % len(colors)
                    sheet_datasets.append({
                        'label': column,
                        'data': data,
                        'backgroundColor': colors[color_idx][0],
                        'borderColor': colors[color_idx][1],
                        'borderWidth': 1,
                        'type': 'bar',
                        'yAxisID': 'y-left'
                    })
            for i, column in enumerate(y_right_cols):
                if column in value_columns:
                    data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                    color_idx = (i + len(y_left_cols)) % len(colors)
                    sheet_datasets.append({
                        'label': column,
                        'data': data,
                        'backgroundColor': colors[color_idx][0],
                        'borderColor': colors[color_idx][1],
                        'borderWidth': 2,
                        'type': 'line',
                        'yAxisID': 'y-right',
                        'fill': False
                    })
        elif sheet == "留存":
            y_left_cols = ["首存人数", "3日留存人数", "7日留存人数", "15日留存人数", "30日留存人数"]
            for i, column in enumerate(y_left_cols):
                if column in value_columns:
                    data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                    color_idx = i % len(colors)
                    sheet_datasets.append({
                        'label': column,
                        'data': data,
                        'backgroundColor': colors[color_idx][0],
                        'borderColor': colors[color_idx][1],
                        'borderWidth': 1,
                        'type': 'bar',
                        'yAxisID': 'y-left'
                    })
        elif sheet in ["存款", "取款"]:
            y_left_cols = ["订单数", "成功数量"]
            time_col = "处理时间"
            for i, column in enumerate(y_left_cols):
                if column in value_columns:
                    data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                    color_idx = i % len(colors)
                    sheet_datasets.append({
                        'label': column,
                        'data': data,
                        'backgroundColor': colors[color_idx][0],
                        'borderColor': colors[color_idx][1],
                        'borderWidth': 1,
                        'type': 'bar',
                        'yAxisID': 'y-left'
                    })
            if time_col in sheet_df.columns:
                sheet_df[time_col] = sheet_df[time_col].apply(time_to_seconds)
                data = sheet_df.groupby(x_axis_col)[time_col].mean().reindex(sheet_date_labels).fillna(0).tolist()
                sheet_datasets.append({
                    'label': time_col,
                    'data': data,
                    'backgroundColor': 'rgba(255, 159, 64, 0.6)',
                    'borderColor': 'rgba(255, 159, 64, 1)',
                    'borderWidth': 2,
                    'type': 'line',
                    'yAxisID': 'y-right',
                    'fill': False
                })

        # 筛选选项
        filter_col = "时间段" if sheet in ["存款", "取款"] else "站点"
        if filter_col in sheet_df.columns:
            site_ids = ['汇总'] + sheet_df[filter_col].dropna().unique().tolist() if filter_col == "站点" else \
            sheet_df[filter_col].dropna().unique().tolist()
        else:
            print(f"警告：Sheet {sheet} 未找到 '{filter_col}' 列")
            site_ids = ['汇总'] if filter_col == "站点" else []

        all_data[sheet] = {
            'labels': sheet_date_labels,
            'datasets': sheet_datasets,
            'raw_df': sheet_df.to_json(orient='records', date_format='iso'),
            'site_ids': site_ids,
            'x_axis_col': x_axis_col,
            'filter_col': filter_col
        }

    # CSS 样式（保持原样）
    css = """
body { font-family: sans-serif; margin: 0; padding: 0; background-color: #000; color: #fff; display: flex; min-height: 100vh; }
* { scrollbar-width: none; -ms-overflow-style: none; }
*::-webkit-scrollbar { display: none; }
.sidebar { width: 133px; background-color: #222; color: #fff; padding: 20px; border-right: 1px solid #444; display: flex; flex-direction: column; position: fixed; top: 0; bottom: 0; overflow-y: auto; }
.sidebar h2 { color: #eee; margin-top: 0; margin-bottom: 15px; }
.sidebar ul { list-style: none; padding: 0; margin: 0; }
.sidebar li a { display: block; padding: 10px 15px; text-decoration: none; color: #ddd; border-radius: 5px; margin-bottom: 5px; cursor: pointer; }
.sidebar li a:hover { background-color: #555; color: #fff; }
.sidebar li a.active { background-color: #777; color: #fff; }
.pivot-view { flex-grow: 1; padding: 20px; overflow: auto; background-color: #111; display: flex; flex-direction: column; gap: 20px; margin-left: 173px; }
.filter-container { background-color: #333; padding: 10px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3); display: flex; gap: 10px; align-items: center; position: fixed; top: 0; left: 173px; right: 0; z-index: 10; }
.filter-container button { background-color: #555; color: #eee; border: none; padding: 8px 15px; border-radius: 3px; cursor: pointer; }
.filter-container button:hover { background-color: #777; }
.filter-container button.active { background-color: #999; color: #fff; }
.chart-container { width: 100%; background-color: #333; border-radius: 5px; box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3); padding: 10px; box-sizing: border-box; display: none; margin-top: 60px; }
.chart-container.active { display: block; }
 -webkit-box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3); }
.chart-container h2 { color: #eee; margin-top: 0; }
.chart-container canvas { width: 100%; max-height: 400px; }
.table-container { width: 100%; max-width: 100%; background-color: #333; border-radius: 5px; box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3); padding: 10px; box-sizing: border-box; font-size: 12px; position: relative; }
.table-header-window { width: 100%; background-color: #333; border-bottom: 1px solid #555; position: sticky; top: 60px; z-index: 10; }
.table-header-scroll { width: 100%; overflow-x: auto; }
.table-body-scroll { width: 100%; max-height: 500px; overflow-x: auto; overflow-y: auto; }
.table-container table { width: 100%; border-collapse: collapse; color: #eee; table-layout: fixed; }
.table-container th, .table-container td { padding: 6px; text-align: left; border-bottom: 1px solid #555; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.table-container th { background-color: #444; }
#loading { position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background-color: rgba(0, 0, 0, 0.8); padding: 20px; border-radius: 5px; z-index: 100; }
@media (max-width: 768px) {
    .sidebar { width: 100px; }
    .pivot-view { margin-left: 140px; }
    .filter-container { left: 140px; flex-wrap: wrap; }
    .table-header-window { top: 60px; }
    .table-body-scroll { max-height: 400px; }
}
.chart-legend { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 20px; }
.chart-legend .legend-group { display: flex; flex-direction: column; gap: 5px; }
.chart-legend .legend-group h4 { margin: 0; color: #fff; font-size: 14px; }
.chart-legend .legend-items { display: flex; flex-wrap: wrap; gap: 10px; }
.chart-legend .legend-item { display: flex; align-items: center; gap: 5px; }
.chart-legend .legend-color { width: 15px; height: 15px; border-radius: 3px; }
"""

    # JavaScript 逻辑（修改部分）
    js = """
function formatSeconds(seconds) {
    var hours = Math.floor(seconds / 3600);
    var minutes = Math.floor((seconds % 3600) / 60);
    var secs = Math.floor(seconds % 60);
    return [
        hours.toString().padStart(2, '0'),
        minutes.toString().padStart(2, '0'),
        secs.toString().padStart(2, '0')
    ].join(':');
}

function formatSecondsToMMSS(seconds) {
    var minutes = Math.floor(seconds / 60);
    var secs = Math.floor(seconds % 60);
    return minutes.toString().padStart(2, '0') + ':' + secs.toString().padStart(2, '0');
}

function formatNumber(value, isSuccessRate, isTime) {
    if (isTime) {
        return formatSecondsToMMSS(value);
    }
    if (isSuccessRate) {
        return (Number(value)*100).toFixed(2) + '%';
    }
    return Number(value).toLocaleString('en-US');
}

var allData = """ + json.dumps(all_data, cls=TimestampEncoder) + """;
var charts = {};
var currentSheet = '""" + default_sheet + """';

function initCharts() {
    Object.keys(allData).forEach(sheet => {
        var canvasId = sheet + '-chart';
        createChart(canvasId, allData[sheet], 'bar');
    });
    toggleDataView('""" + default_sheet + """-container');
}

function createChart(canvasId, data, type) {
    var ctx = document.getElementById(canvasId).getContext('2d');
    var options = {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            'y-left': {
                beginAtZero: true,
                position: 'left',
                ticks: { 
                    color: '#fff',
                    callback: function(value) {
                        return value.toLocaleString('en-US');
                    }
                },
                grid: { display: true },
                title: { display: true, text: '数值', color: '#fff' }
            },
            x: { ticks: { color: '#fff' } }
        },
        plugins: {
            legend: { display: false }
        }
    };
    if (canvasId === '存款-chart' || canvasId === '取款-chart' || canvasId === '人数-chart' || canvasId === '金额-chart') {
        options.scales['y-right'] = {
            beginAtZero: true,
            position: 'right',
            ticks: {
                color: '#fff',
                callback: function(value) {
                    if (canvasId === '存款-chart' || canvasId === '取款-chart') {
                        return formatSecondsToMMSS(value);
                    }
                    return value.toLocaleString('en-US');
                }
            },
            grid: { display: false },
            title: { 
                display: true, 
                text: canvasId === '存款-chart' || canvasId === '取款-chart' ? '处理时间 (MM:SS)' : '数值', 
                color: '#fff' 
            }
        };
    }

    charts[canvasId] = new Chart(ctx, {
        type: type,
        data: data,
        options: options
    });

    var chartContainer = document.getElementById(canvasId).parentElement;
    var legendContainer = document.createElement('div');
    legendContainer.className = 'chart-legend';
    var leftGroup = { title: '左侧柱状图', items: [] };
    var rightGroup = { title: '右侧折线图', items: [] };

    data.datasets.forEach(dataset => {
        var group = dataset.yAxisID === 'y-right' ? rightGroup : leftGroup;
        group.items.push({
            label: dataset.label,
            color: dataset.backgroundColor
        });
    });

    if (leftGroup.items.length > 0) {
        var leftGroupHtml = '<div class="legend-group"><h4>' + leftGroup.title + '</h4><div class="legend-items">';
        leftGroup.items.forEach(item => {
            leftGroupHtml += '<div class="legend-item"><div class="legend-color" style="background-color: ' + item.color + ';"></div>' + item.label + '</div>';
        });
        leftGroupHtml += '</div></div>';
        legendContainer.innerHTML += leftGroupHtml;
    }

    if (rightGroup.items.length > 0) {
        var rightGroupHtml = '<div class="legend-group"><h4>' + rightGroup.title + '</h4><div class="legend-items">';
        rightGroup.items.forEach(item => {
            rightGroupHtml += '<div class="legend-item"><div class="legend-color" style="background-color: ' + item.color + ';"></div>' + item.label + '</div>';
        });
        rightGroupHtml += '</div></div>';
        legendContainer.innerHTML += rightGroupHtml;
    }

    chartContainer.appendChild(legendContainer);
}

function updateTable(data, filteredData) {
    var tableBody = document.getElementById('data-table').getElementsByTagName('tbody')[0];
    var headerRow = document.getElementById('data-table-header').getElementsByTagName('thead')[0].getElementsByTagName('tr')[0];
    var xAxisCol = allData[currentSheet].x_axis_col;
    var filterCol = allData[currentSheet].filter_col;
    var columns = Object.keys(filteredData[0] || {}).filter(col => col !== xAxisCol && col !== filterCol);

    tableBody.innerHTML = '';
    headerRow.innerHTML = '<th>' + xAxisCol + '</th>' + columns.map(col => '<th>' + col + '</th>').join('');

    var groupedData = {};
    filteredData.forEach(row => {
        var xAxis = row[xAxisCol];
        if (!groupedData[xAxis]) groupedData[xAxis] = {};
        columns.forEach(col => {
            var value = row[col];
            if (col === '处理时间') {
                value = value !== undefined ? parseFloat(value) : 0;
                groupedData[xAxis][col] = (groupedData[xAxis][col] || []).concat([value]);
            } else {
                value = value !== undefined ? parseFloat(value) : 0;
                groupedData[xAxis][col] = (groupedData[xAxis][col] || 0) + value;
            }
        });
    });

    data.labels.forEach((label, index) => {
        var row = document.createElement('tr');
        var dateCell = document.createElement('td');
        dateCell.textContent = label;
        row.appendChild(dateCell);
        columns.forEach(col => {
            var cell = document.createElement('td');
            var value = groupedData[label] && groupedData[label][col] !== undefined ? groupedData[label][col] : 0;
            if (col === '处理时间' && groupedData[label]) {
                var values = groupedData[label][col] || [];
                value = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
            }
            var isSuccessRate = col === '成功率' && (currentSheet === '存款' || currentSheet === '取款');
            var isTime = col === '处理时间';
            cell.textContent = formatNumber(value, isSuccessRate, isTime);
            row.appendChild(cell);
        });
        tableBody.appendChild(row);
    });

    var headerScroll = document.querySelector('.table-header-scroll');
    var bodyScroll = document.querySelector('.table-body-scroll');
    headerScroll.onscroll = function () {
        bodyScroll.scrollLeft = headerScroll.scrollLeft;
    };
    bodyScroll.onscroll = function () {
        headerScroll.scrollLeft = bodyScroll.scrollLeft;
    };
}

function toggleDataView(viewId, element) {
    var chartContainers = document.querySelectorAll('.chart-container');
    chartContainers.forEach(container => container.classList.remove('active'));
    var selectedView = document.getElementById(viewId);
    if (selectedView) selectedView.classList.add('active');
    var sidebarLinks = document.querySelectorAll('.sidebar li a');
    sidebarLinks.forEach(link => link.classList.remove('active'));
    if (element) element.classList.add('active');
    currentSheet = viewId.split('-')[0];
    var defaultFilter = allData[currentSheet].site_ids[0] || '';
    filterData(defaultFilter, null);
    document.querySelector('.filter-container').innerHTML = allData[currentSheet].site_ids.map(id =>
        '<button onclick="filterData(\\'' + id + '\\', this)">' + id + '</button>'
    ).join('');
    var firstButton = document.querySelector('.filter-container button');
    if (firstButton) firstButton.classList.add('active');
}

function filterData(filterKey, element) {
    var newData = { labels: allData[currentSheet].labels, datasets: [] };
    var rawDf = JSON.parse(allData[currentSheet].raw_df);
    var xAxisCol = allData[currentSheet].x_axis_col;
    var filterCol = allData[currentSheet].filter_col;
    var filteredData = rawDf;

    if (currentSheet === '人数' || currentSheet === '金额' || currentSheet === '留存') {
        if (filterKey !== '汇总') {
            filteredData = rawDf.filter(row => String(row[filterCol]) === String(filterKey));
        }
    } else if (currentSheet === '存款' || currentSheet === '取款') {
        filteredData = rawDf.filter(row => String(row[filterCol]) === String(filterKey));
    }

    var groupedData = {};
    filteredData.forEach(row => {
        var xAxis = row[xAxisCol];
        if (!groupedData[xAxis]) groupedData[xAxis] = {};
        allData[currentSheet].datasets.forEach(dataset => {
            var value = row[dataset.label];
            if (dataset.label === '处理时间') {
                value = value !== undefined ? parseFloat(value) : 0;
                groupedData[xAxis][dataset.label] = (groupedData[xAxis][dataset.label] || []).concat([value]);
            } else {
                value = value !== undefined ? parseFloat(value) : 0;
                groupedData[xAxis][dataset.label] = (groupedData[xAxis][dataset.label] || 0) + value;
            }
        });
    });

    allData[currentSheet].datasets.forEach(dataset => {
        var data = allData[currentSheet].labels.map(xAxis => {
            if (groupedData[xAxis]) {
                if (dataset.label === '处理时间') {
                    var values = groupedData[xAxis][dataset.label] || [];
                    return values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
                }
                return groupedData[xAxis][dataset.label] || 0;
            }
            return 0;
        });
        newData.datasets.push({
            label: dataset.label,
            data: data,
            backgroundColor: dataset.backgroundColor,
            borderColor: dataset.borderColor,
            borderWidth: dataset.borderWidth,
            type: dataset.type,
            yAxisID: dataset.yAxisID,
            fill: dataset.fill
        });
    });

    updateChart(currentSheet + '-chart', newData, filteredData);
    var filterButtons = document.querySelectorAll('.filter-container button');
    filterButtons.forEach(btn => btn.classList.remove('active'));
    if (element) element.classList.add('active');
}

function updateChart(canvasId, newData, filteredData) {
    if (charts[canvasId]) {
        charts[canvasId].data = newData;
        charts[canvasId].update();
        var chartContainer = document.getElementById(canvasId).parentElement;
        var legendContainer = chartContainer.querySelector('.chart-legend');
        if (legendContainer) {
            legendContainer.remove();
        }
        legendContainer = document.createElement('div');
        legendContainer.className = 'chart-legend';
        var leftGroup = { title: '左侧柱状图', items: [] };
        var rightGroup = { title: '右侧折线图', items: [] };

        newData.datasets.forEach(dataset => {
            var group = dataset.yAxisID === 'y-right' ? rightGroup : leftGroup;
            group.items.push({
                label: dataset.label,
                color: dataset.backgroundColor
            });
        });

        if (leftGroup.items.length > 0) {
            var leftGroupHtml = '<div class="legend-group"><h4>' + leftGroup.title + '</h4><div class="legend-items">';
            leftGroup.items.forEach(item => {
                leftGroupHtml += '<div class="legend-item"><div class="legend-color" style="background-color: ' + item.color + ';"></div>' + item.label + '</div>';
            });
            leftGroupHtml += '</div></div>';
            legendContainer.innerHTML += leftGroupHtml;
        }

        if (rightGroup.items.length > 0) {
            var rightGroupHtml = '<div class="legend-group"><h4>' + rightGroup.title + '</h4><div class="legend-items">';
            rightGroup.items.forEach(item => {
                rightGroupHtml += '<div class="legend-item"><div class="legend-color" style="background-color: ' + item.color + ';"></div>' + item.label + '</div>';
            });
            rightGroupHtml += '</div></div>';
            legendContainer.innerHTML += rightGroupHtml;
        }

        chartContainer.appendChild(legendContainer);
    }
    updateTable(newData, filteredData);
}

document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('loading').style.display = 'block';
    initCharts();
    document.getElementById('loading').style.display = 'none';
});
"""

    # HTML 生成函数
    def generate_html(sheets, all_data, default_sheet, css, js):
        sidebar_items = "\n".join(
            [f'<li><a href="#" onclick="toggleDataView(\'{sheet}-container\', this)">{sheet}</a></li>' for sheet in
             sheets]
        )

        chart_containers = "\n".join(
            [f'''
<div id="{sheet}-container" class="chart-container">
    <h2>{sheet} 数据图</h2>
    <canvas id="{sheet}-chart"></canvas>
</div>''' for sheet in sheets]
        )

        filter_buttons = "\n".join(
            [f'<button onclick="filterData(\'{site_id}\', this)">{site_id}</button>' for site_id in
             all_data[default_sheet]['site_ids']]
        )

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>数据透视图</title>
    <style>
{css}
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
{js}
    </script>
</head>
<body>
    <div class="sidebar">
        <h2>GD</h2>
        <ul>
{sidebar_items}
        </ul>
    </div>
    <div class="pivot-view">
        <div class="filter-container">
{filter_buttons}
        </div>
{chart_containers}
        <div class="table-container">
            <div class="table-header-window">
                <h2>数据表格</h2>
                <div class="table-header-scroll">
                    <table id="data-table-header">
                        <thead><tr></tr></thead>
                    </table>
                </div>
            </div>
            <div class="table-body-scroll">
                <table id="data-table">
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </div>
    <div id="loading" style="display: none;">加载中...</div>
</body>
</html>
"""
        return html

    html_content = generate_html(sheets, all_data, default_sheet, css, js)
    with open("excel_pivot_view.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("excel_pivot_view.html 文件已生成，包含所有功能和界面。")

    return html_content


if __name__ == "__main__":
    generate_excel_pivot_view_html()
