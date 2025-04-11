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
    """将时间字符串（hh:mm:ss 或其他格式）转换为秒数"""
    try:
        if isinstance(time_str, str):
            parts = time_str.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = map(int, parts)
                return hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 2:
                minutes, seconds = map(int, parts)
                return minutes * 60 + seconds
            elif len(parts) == 1:
                return int(parts[0])
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
                print(f"警告：Sheet {sheet} 的列数不足，至少需要 3 列，跳过处理")
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

def generate_excel_pivot_view_html_optimized():
    sheets_data = load_all_sheets("30日报表.xlsx")
    if sheets_data is None:
        return "<h1>错误：数据加载失败</h1>"

    sheets = list(sheets_data.keys())
    print(f"加载的 sheet：{sheets}")
    default_sheet = sheets[0]
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
        sheet_date_labels = sheet_df.iloc[:, 1].unique().tolist() if len(sheet_df.columns) > 1 else []
        value_columns = [col for col in sheet_df.columns[2:] if pd.api.types.is_numeric_dtype(sheet_df[col])]
        sheet_datasets = []

        if sheet == "支付统计" and "平均处理时间" in sheet_df.columns:
            # 分离“平均处理时间”和其他数值列
            avg_time_col = "平均处理时间"
            other_value_columns = [col for col in value_columns if col != avg_time_col]
            # 处理其他数值列（柱状图，左侧 Y 轴）
            for i, column in enumerate(other_value_columns):
                data = sheet_df.groupby(sheet_df.columns[1])[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
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
            # 处理“平均处理时间”（折线图，右侧 Y 轴）
            if avg_time_col in sheet_df.columns:
                # 将时间字符串转换为秒数
                sheet_df[avg_time_col] = sheet_df[avg_time_col].apply(time_to_seconds)
                data = sheet_df.groupby(sheet_df.columns[1])[avg_time_col].mean().reindex(sheet_date_labels).fillna(0).tolist()
                sheet_datasets.append({
                    'label': avg_time_col,
                    'data': data,
                    'backgroundColor': 'rgba(255, 159, 64, 0.6)',
                    'borderColor': 'rgba(255, 159, 64, 1)',
                    'borderWidth': 2,
                    'type': 'line',
                    'yAxisID': 'y-right',
                    'fill': False
                })
        else:
            # 其他 sheet 保持原有逻辑
            for i, column in enumerate(value_columns):
                data = sheet_df.groupby(sheet_df.columns[1])[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                color_idx = i % len(colors)
                sheet_datasets.append({
                    'label': column,
                    'data': data,
                    'backgroundColor': colors[color_idx][0],
                    'borderColor': colors[color_idx][1],
                    'borderWidth': 1,
                    'type': 'bar'
                })

        site_ids = sheet_df.iloc[:, 0].unique().tolist() if sheet == "支付统计" and len(sheet_df.columns) > 0 else ['汇总'] + sheet_df.iloc[:, 0].unique().tolist()
        # 移除“支付统计”中的“汇总”选项
        if sheet == "支付统计":
            site_ids = [sid for sid in site_ids if sid != '汇总']

        all_data[sheet] = {
            'labels': sheet_date_labels,
            'datasets': sheet_datasets,
            'raw_df': sheet_df.to_json(orient='records', date_format='iso'),
            'site_ids': site_ids,
            'date_column_name': sheet_df.columns[1]  # 保存日期列名称
        }
    print(f"all_data 中的 sheet：{list(all_data.keys())}")

    html = """
<!DOCTYPE html>
<html>
<head>
  <title>数据透视图</title>
  <style>
    body { font-family: sans-serif; margin: 0; padding: 0; background-color: #000; color: #fff; display: flex; min-height: 100vh; }
    .sidebar { width: 200px; background-color: #222; color: #fff; padding: 20px; border-right: 1px solid #444; display: flex; flex-direction: column; position: fixed; top: 0; bottom: 0; overflow-y: auto; }
    .sidebar h2 { color: #eee; margin-top: 0; margin-bottom: 15px; }
    .sidebar ul { list-style: none; padding: 0; margin: 0; }
    .sidebar li a { display: block; padding: 10px 15px; text-decoration: none; color: #ddd; border-radius: 5px; margin-bottom: 5px; cursor: pointer; }
    .sidebar li a:hover { background-color: #555; color: #fff; }
    .sidebar li a.active { background-color: #777; color: #fff; }
    .pivot-view { flex-grow: 1; padding: 20px; overflow: auto; background-color: #111; display: flex; flex-direction: column; gap: 20px; margin-left: 240px; }
    .filter-container { background-color: #333; padding: 10px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3); display: flex; gap: 10px; align-items: center; position: fixed; top: 0; left: 240px; right: 0; z-index: 10; }
    .filter-container button { background-color: #555; color: #eee; border: none; padding: 8px 15px; border-radius: 3px; cursor: pointer; }
    .filter-container button:hover { background-color: #777; }
    .filter-container button.active { background-color: #999; color: #fff; }
    .chart-container { width: 100%; background-color: #333; border-radius: 5px; box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3); padding: 10px; box-sizing: border-box; display: none; margin-top: 60px; }
    .chart-container.active { display: block; }
    .chart-container h2 { color: #eee; margin-top: 0; }
    .chart-container canvas { width: 100%; max-height: 400px; }
    .table-container { 
      width: 100%; 
      max-width: 100%; 
      background-color: #333; 
      border-radius: 5px; 
      box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3); 
      padding: 10px; 
      box-sizing: border-box; 
      font-size: 12px; 
      max-height: 400px; 
      overflow-y: auto; 
    }
    .table-container table { 
      width: 100%; 
      border-collapse: collapse; 
      color: #eee; 
      table-layout: fixed; 
    }
    .table-container thead { 
      position: sticky; 
      top: 0; 
      background-color: #444; 
      z-index: 5; 
    }
    .table-container th, .table-container td { 
      padding: 6px; 
      text-align: left; 
      border-bottom: 1px solid #555; 
      white-space: nowrap; 
      overflow: hidden; 
      text-overflow: ellipsis; 
    }
    .table-container th { background-color: #444; }
    #loading { position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background-color: rgba(0, 0, 0, 0.8); padding: 20px; border-radius: 5px; z-index: 100; }
    @media (max-width: 768px) { 
      .sidebar { width: 150px; } 
      .pivot-view { margin-left: 170px; } 
      .filter-container { left: 170px; flex-wrap: wrap; } 
      .table-container { max-height: 300px; }
    }
  </style>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
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

    var allData = """ + json.dumps(all_data, cls=TimestampEncoder) + """;
    var charts = {};
    var currentSheet = '""" + default_sheet + """';

    function initCharts() {
      Object.keys(allData).forEach(sheet => {
        var canvasId = sheet + '-chart';
        var chartType = sheet === '支付统计' ? 'bar' : 'bar';
        createChart(canvasId, allData[sheet], chartType);
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
            ticks: { color: '#fff' },
            grid: { display: true },
            title: { display: true, text: '数值', color: '#fff' }
          },
          x: { ticks: { color: '#fff' } }
        },
        plugins: { legend: { labels: { color: '#fff' } } }
      };
      if (canvasId === '支付统计-chart') {
        options.scales['y-right'] = { 
          beginAtZero: true, 
          position: 'right', 
          ticks: { color: '#fff' },
          grid: { display: false },
          title: { display: true, text: '平均处理时间 (秒)', color: '#fff' }
        };
      }
      charts[canvasId] = new Chart(ctx, {
        type: type,
        data: data,
        options: options
      });
    }

    function updateChart(canvasId, newData) {
      if (charts[canvasId]) {
        charts[canvasId].data = newData;
        charts[canvasId].update();
      }
      updateTable(newData);
    }

    function updateTable(data) {
      var tableBody = document.getElementById('data-table').getElementsByTagName('tbody')[0];
      tableBody.innerHTML = '';
      var headerRow = document.getElementById('data-table').getElementsByTagName('thead')[0].getElementsByTagName('tr')[0];
      headerRow.innerHTML = '<th>' + allData[currentSheet].date_column_name + '</th>' + data.datasets.map(ds => '<th>' + ds.label + '</th>').join('');
      data.labels.forEach((label, index) => {
        var row = document.createElement('tr');
        var dateCell = document.createElement('td');
        dateCell.textContent = label;
        row.appendChild(dateCell);
        data.datasets.forEach(dataset => {
          var cell = document.createElement('td');
          var value = dataset.data[index];
          cell.textContent = dataset.label === '平均处理时间' ? formatSeconds(value) : value;
          row.appendChild(cell);
        });
        tableBody.appendChild(row);
      });
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
      updateTable(allData[currentSheet]);
      document.querySelector('.filter-container').innerHTML = allData[currentSheet].site_ids.map(id => 
        '<button onclick="filterData(\\'' + id + '\\', this)">' + id + '</button>'
      ).join('');
      // 默认激活第一个筛选按钮（如果有）
      var firstButton = document.querySelector('.filter-container button');
      if (firstButton) firstButton.classList.add('active');
    }

    function filterData(filterKey, element) {
      var newData = { labels: allData[currentSheet].labels, datasets: [] };
      var rawDf = JSON.parse(allData[currentSheet].raw_df);
      var hasSiteId = rawDf.some(row => allData[currentSheet].site_ids.includes(row[Object.keys(row)[0]]));
      var filteredData = hasSiteId && filterKey ? rawDf.filter(row => String(row[Object.keys(row)[0]]) === String(filterKey)) : rawDf;

      var groupedData = {};
      filteredData.forEach(row => {
        var date = row[Object.keys(row)[1]];
        if (!groupedData[date]) groupedData[date] = {};
        allData[currentSheet].datasets.forEach(dataset => {
          var value = row[dataset.label];
          if (dataset.label === '平均处理时间') {
            value = value !== undefined ? parseFloat(value) : 0;
            groupedData[date][dataset.label] = (groupedData[date][dataset.label] || []).concat([value]);
          } else {
            value = value !== undefined ? parseFloat(value) : 0;
            groupedData[date][dataset.label] = (groupedData[date][dataset.label] || 0) + value;
          }
        });
      });

      allData[currentSheet].datasets.forEach(dataset => {
        var data = allData[currentSheet].labels.map(date => {
          if (groupedData[date]) {
            if (dataset.label === '平均处理时间') {
              var values = groupedData[date][dataset.label] || [];
              return values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
            }
            return groupedData[date][dataset.label] || 0;
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
          yAxisID: dataset.yAxisID || (dataset.label === '平均处理时间' ? 'y-right' : 'y-left'),
          fill: dataset.fill
        });
      });

      updateChart(currentSheet + '-chart', newData);
      var filterButtons = document.querySelectorAll('.filter-container button');
      filterButtons.forEach(btn => btn.classList.remove('active'));
      if (element) element.classList.add('active');
    }

    document.addEventListener('DOMContentLoaded', function() {
      document.getElementById('loading').style.display = 'block';
      initCharts();
      document.getElementById('loading').style.display = 'none';
      var firstButton = document.querySelector('.filter-container button');
      if (firstButton) firstButton.classList.add('active');
    });
  </script>
</head>
<body>
  <div class="sidebar">
    <h2>GD</h2>
    <ul>
""" + "\n".join([f"      <li><a href=\"#\" onclick=\"toggleDataView('{sheet}-container', this)\">{sheet}</a></li>" for sheet in sheets]) + """
      <li><a href="#">筛选器</a></li>
      <li><a href="#">行标签</a></li>
      <li><a href="#">列标签</a></li>
      <li><a href="#">数值</a></li>
      <li><a href="#">图表选项</a></li>
      <li><a href="#">刷新数据</a></li>
      <li><a href="#" onclick="toggleDataView('""" + default_sheet + """-container', this)">字段列表</a></li>
    </ul>
  </div>
  <div class="pivot-view">
    <div class="filter-container">
""" + "\n".join([f"      <button onclick=\"filterData('{site_id}', this)\">{site_id}</button>" for site_id in all_data[default_sheet]['site_ids']]) + """
    </div>
""" + "\n".join([f"""
    <div id="{sheet}-container" class="chart-container">
      <h2>{sheet} 数据图</h2>
      <canvas id="{sheet}-chart"></canvas>
    </div>""" for sheet in sheets]) + """
    <div class="table-container">
      <h2>数据表格</h2>
      <table id="data-table">
        <thead><tr></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
  <div id="loading" style="display: none;">加载中...</div>
</body>
</html>
"""
    return html

if __name__ == "__main__":
    pivot_view_html = generate_excel_pivot_view_html_optimized()
    with open("excel_pivot_view_optimized.html", "w", encoding="utf-8") as f:
        f.write(pivot_view_html)
    print("excel_pivot_view_optimized.html 文件已生成，包含优化后的功能和界面。")
