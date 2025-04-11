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

def load_all_sheets(file_path):
    """加载所有 sheet 数据"""
    try:
        excel_file = pd.ExcelFile(file_path)
        print(f"找到的 sheet 名称：{excel_file.sheet_names}")
        sheets_data = {sheet: pd.read_excel(file_path, sheet_name=sheet)
                       for sheet in excel_file.sheet_names}
        return sheets_data
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
        # 使用 B 列（第二列）作为 X 轴标签
        sheet_date_labels = sheet_df.iloc[:, 1].unique().tolist() if len(sheet_df.columns) > 1 else []
        # 数值列从第 C 列（第三列）开始
        value_columns = [col for col in sheet_df.columns[2:] if pd.api.types.is_numeric_dtype(sheet_df[col])]
        sheet_datasets = []
        for i, column in enumerate(value_columns):
            # 使用 B 列分组
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
        all_data[sheet] = {
            'labels': sheet_date_labels,
            'datasets': sheet_datasets,
            'raw_df': sheet_df.to_json(orient='records', date_format='iso'),
            # 使用 A 列（第一列）作为筛选条件
            'site_ids': ['汇总'] + sheet_df.iloc[:, 0].unique().tolist() if len(sheet_df.columns) > 0 else ['汇总']
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
    }
    .table-container table { 
      width: 100%; 
      border-collapse: collapse; 
      color: #eee; 
      table-layout: fixed; /* 固定表格布局，自动分配列宽 */
    }
    .table-container th, .table-container td { 
      padding: 6px; 
      text-align: left; 
      border-bottom: 1px solid #555; 
      white-space: nowrap; /* 保持内容不换行 */
      overflow: hidden; /* 隐藏超出部分 */
      text-overflow: ellipsis; /* 用省略号表示超出内容 */
    }
    .table-container th { background-color: #444; }
    #loading { position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background-color: rgba(0, 0, 0, 0.8); padding: 20px; border-radius: 5px; z-index: 100; }
    @media (max-width: 768px) { .sidebar { width: 150px; } .pivot-view { margin-left: 170px; } .filter-container { left: 170px; flex-wrap: wrap; } }
  </style>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
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
      charts[canvasId] = new Chart(ctx, {
        type: type,
        data: data,
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: { y: { beginAtZero: true, ticks: { color: '#fff' } }, x: { ticks: { color: '#fff' } } },
          plugins: { legend: { labels: { color: '#fff' } } }
        }
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
      headerRow.innerHTML = '<th>' + allData[currentSheet].labels[0] + '</th>' + data.datasets.map(ds => '<th>' + ds.label + '</th>').join('');
      data.labels.forEach((label, index) => {
        var row = document.createElement('tr');
        var dateCell = document.createElement('td');
        dateCell.textContent = label;
        row.appendChild(dateCell);
        data.datasets.forEach(dataset => {
          var cell = document.createElement('td');
          cell.textContent = dataset.data[index];
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
    }

    function filterData(filterKey, element) {
      var newData = { labels: allData[currentSheet].labels, datasets: [] };
      var rawDf = JSON.parse(allData[currentSheet].raw_df);
      var hasSiteId = rawDf.some(row => allData[currentSheet].site_ids.includes(row[Object.keys(row)[0]]));
      var filteredData = hasSiteId && filterKey !== '汇总' ? rawDf.filter(row => String(row[Object.keys(row)[0]]) === String(filterKey)) : rawDf;

      var groupedData = {};
      filteredData.forEach(row => {
        var date = row[Object.keys(row)[1]]; // 使用 B 列
        if (!groupedData[date]) groupedData[date] = {};
        allData[currentSheet].datasets.forEach(dataset => {
          groupedData[date][dataset.label] = (groupedData[date][dataset.label] || 0) + (row[dataset.label] || 0);
        });
      });

      allData[currentSheet].datasets.forEach(dataset => {
        var data = allData[currentSheet].labels.map(date => groupedData[date] ? (groupedData[date][dataset.label] || 0) : 0);
        newData.datasets.push({
          label: dataset.label,
          data: data,
          backgroundColor: dataset.backgroundColor,
          borderColor: dataset.borderColor,
          borderWidth: dataset.borderWidth,
          type: dataset.type
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
      document.querySelector('.filter-container button').classList.add('active');
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
