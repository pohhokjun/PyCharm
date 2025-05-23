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

def generate_0_Ultimate_Analysis_html():
   sheets_data = load_all_sheets("0_Ultimate_Analysis.xlsx")
   if sheets_data is None:
       return "<h1>错误：数据加载失败</h1>"

   sheets = list(sheets_data.keys())
   print(f"加载的 sheet：{sheets}")
   default_sheet = sheets[0] if sheets else ""
   colors = [
       ('rgba(54, 162, 235, 0.6)', 'rgba(54, 162, 235, 1)'),  # 蓝色
       ('rgba(255, 99, 132, 0.6)', 'rgba(255, 99, 132, 1)'),  # 红色
       ('rgba(255, 206, 86, 0.6)', 'rgba(255, 206, 86, 1)'),  # 黄色
       ('rgba(75, 192, 192, 0.6)', 'rgba(75, 192, 192, 1)'),  # 青色
       ('rgba(153, 102, 255, 0.6)', 'rgba(153, 102, 255, 1)'),  # 紫色
       ('rgba(255, 159, 64, 0.6)', 'rgba(255, 159, 64, 1)'),  # 橙色
       ('rgba(128, 128, 128, 0.6)', 'rgba(128, 128, 128, 1)'),  # 灰色
       ('rgba(0, 255, 127, 0.6)', 'rgba(0, 255, 127, 1)'),  # 春绿色
       ('rgba(199, 21, 133, 0.6)', 'rgba(199, 21, 133, 1)'),  # 洋红色
       ('rgba(50, 205, 50, 0.6)', 'rgba(50, 205, 50, 1)'),  # 酸橙色
       ('rgba(255, 105, 180, 0.6)', 'rgba(255, 105, 180, 1)'),  # 粉红色
       ('rgba(139, 69, 19, 0.6)', 'rgba(139, 69, 19, 1)'),  # 棕色
       ('rgba(30, 144, 255, 0.6)', 'rgba(30, 144, 255, 1)'),  # 道奇蓝
       ('rgba(255, 215, 0, 0.6)', 'rgba(255, 215, 0, 1)'),  # 金色
       ('rgba(0, 206, 209, 0.6)', 'rgba(0, 206, 209, 1)')  # 碧绿色
   ]
   deposit_withdrawal_colors = {
       '7日订单金额': ('rgba(255, 99, 132, 0.6)', 'rgba(255, 99, 132, 1)'),  # 浅红色
       '7日成功金额': ('rgba(200, 50, 100, 1)', 'rgba(200, 50, 100, 1)'),  # 深红色
       '30日订单金额': ('rgba(54, 162, 235, 0.6)', 'rgba(54, 162, 235, 1)'),  # 浅蓝色
       '30日成功金额': ('rgba(30, 120, 200, 1)', 'rgba(30, 120, 200, 1)'),  # 深蓝色
       '7日订单数': ('rgba(255, 99, 132, 0.6)', 'rgba(255, 99, 132, 1)'),  # 浅红色
       '7日成功数量': ('rgba(200, 50, 100, 1)', 'rgba(200, 50, 100, 1)'),  # 深红色
       '30日订单数': ('rgba(54, 162, 235, 0.6)', 'rgba(54, 162, 235, 1)'),  # 浅蓝色
       '30日成功数量': ('rgba(30, 120, 200, 1)', 'rgba(30, 120, 200, 1)')  # 深蓝色
   }
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
           y_left_cols = ["公司输赢", "公司净收入", "代理佣金"]
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
           y_left_cols = ["30日订单金额", "30日成功金额", "7日订单金额", "7日成功金额"]
           y_right_cols = ["30日订单数", "30日成功数量", "7日订单数", "7日成功数量"]
           for column in y_left_cols:
               if column in value_columns:
                   data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                   sheet_datasets.append({
                       'label': column,
                       'data': data,
                       'backgroundColor': deposit_withdrawal_colors[column][0],
                       'borderColor': deposit_withdrawal_colors[column][1],
                       'borderWidth': 1,
                       'type': 'bar',
                       'yAxisID': 'y-left'
                   })
           for column in y_right_cols:
               if column in value_columns:
                   data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                   sheet_datasets.append({
                       'label': column,
                       'data': data,
                       'type': 'line',
                       'backgroundColor': deposit_withdrawal_colors[column][0],
                       'borderColor': deposit_withdrawal_colors[column][1],
                       'borderWidth': 2,
                       'yAxisID': 'y-right',
                       'fill': False
                   })
       elif sheet == "红利":
           y_left_cols = [col for col in sheet_df.columns if col.endswith('_金额')]
           y_right_cols = [col for col in sheet_df.columns if col.endswith('_人数')]
           # 创建字段到颜色索引的映射
           field_to_color = {}
           for i, column in enumerate(y_left_cols):
               field = column.replace('_金额', '')  # 提取字段名
               field_to_color[field] = i % len(colors)  # 为字段分配颜色索引
               if column in value_columns:
                   data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
                   color_idx = field_to_color[field]
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
               field = column.replace('_人数', '')  # 提取字段名
               if field in field_to_color:  # 确保使用相同的颜色索引
                   color_idx = field_to_color[field]
               else:
                   color_idx = (i + len(y_left_cols)) % len(colors)  # 备用颜色
               if column in value_columns:
                   data = sheet_df.groupby(x_axis_col)[column].sum().reindex(sheet_date_labels).fillna(0).tolist()
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

       # 筛选选项
       filter_col = "站点"
       if filter_col in sheet_df.columns:
           site_ids = ['汇总'] + sheet_df[filter_col].dropna().unique().tolist()
       else:
           site_ids = ['汇总']

       all_data[sheet] = {
           'labels': sheet_date_labels, # Keep labels for reference
           'datasets': sheet_datasets, # Data is still list of numbers here, will convert in JS
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

   # JavaScript 逻辑
   js = """
function formatNumber(value, isSuccessRate) {
   if (isSuccessRate) {
       return (Number(value) * 100).toFixed(2) + '%';
   }
   return Number(value).toLocaleString('en-US');
}

var allData = """ + json.dumps(all_data, cls=TimestampEncoder) + """;
var charts = {};
var currentSheet = '""" + default_sheet + """';
var currentFilterKey = '汇总'; // 添加一个变量来存储当前的筛选值

function initCharts() {
   Object.keys(allData).forEach(sheet => {
       var canvasId = sheet + '-chart';
       // Initial data for chart creation - filterData will update it later
       createChart(canvasId, { labels: allData[sheet].labels, datasets: [] }, 'bar');
   });
   toggleDataView('""" + default_sheet + """-container');
}

function createChart(canvasId, data, type) {
   var ctx = document.getElementById(canvasId).getContext('2d');
   var options = {
       responsive: true,
       maintainAspectRatio: false,
       scales: {
           // 使用 'category' 类型来统一处理日期和非日期标签
           x: {
               type: 'category',
               ticks: { color: '#fff' }
           },
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
           }
       },
       plugins: {
           legend: { display: false }
       }
   };
   if (canvasId === '存款-chart' || canvasId === '取款-chart' || canvasId === '人数-chart' || canvasId === '金额-chart' || canvasId === '红利-chart') {
       options.scales['y-right'] = {
           beginAtZero: true,
           position: 'right',
           ticks: {
               color: '#fff',
               callback: function(value) {
                   return value.toLocaleString('en-US');
               }
           },
           grid: { display: false },
           title: {
               display: true,
               text: '数值',
               color: '#fff'
           }
       };
   }

   charts[canvasId] = new Chart(ctx, {
       type: type,
       data: data, // Initial empty data, will be populated by filterData
       options: options
   });

   // Legend is added/updated in updateChart
}

function updateTable(data, filteredData, xAxisCol, filterCol) {
   var tableBody = document.getElementById('data-table').getElementsByTagName('tbody')[0];
   var headerRow = document.getElementById('data-table-header').getElementsByTagName('thead')[0].getElementsByTagName('tr')[0];
   // Use keys from the first data point in the chart data to get columns, excluding x and y if present, and filterCol
   var columns = data.datasets.length > 0 && data.datasets[0].data.length > 0
                ? Object.keys(data.datasets[0].data[0]).filter(col => col !== 'x' && col !== 'y')
                : []; // Fallback if no data

   // Get all unique columns from the raw filtered data for the table header
   var allFilteredCols = filteredData.length > 0 ? Object.keys(filteredData[0]) : [];
   var tableColumns = allFilteredCols.filter(col => col !== xAxisCol && col !== filterCol);


   // 设置表头
   headerRow.innerHTML = '<th>' + xAxisCol + '</th>' + tableColumns.map(col => '<th>' + col + '</th>').join('');
   tableBody.innerHTML = '';

   // 按 xAxisCol 汇总数据 for table
   var groupedDataForTable = {};
   filteredData.forEach(row => {
       var xAxis = row[xAxisCol];
       if (!groupedDataForTable[xAxis]) groupedDataForTable[xAxis] = {};
        tableColumns.forEach(col => {
           var value = row[col] !== undefined ? (typeof row[col] === 'number' ? row[col] : parseFloat(row[col]) || 0) : 0;
           groupedDataForTable[xAxis][col] = (groupedDataForTable[xAxis][col] || 0) + value;
       });
   });

    // Use the original labels to ensure all rows are present in the table, even if values are 0
    var tableData = allData[currentSheet].labels.map(xAxis => {
        var row = { [xAxisCol]: xAxis };
        tableColumns.forEach(col => {
            row[col] = groupedDataForTable[xAxis] ? groupedDataForTable[xAxis][col] || 0 : 0;
        });
        return row;
    });


   // 按 xAxisCol 排序 (日期或字符串)
   tableData.sort((a, b) => {
       const valA = a[xAxisCol];
       const valB = b[xAxisCol];
       // Attempt to sort as dates first if they look like ISO strings
       const dateA = new Date(valA);
       const dateB = new Date(valB);
       if (!isNaN(dateA.getTime()) && !isNaN(dateB.getTime())) {
           return dateA.getTime() - dateB.getTime();
       }
       // Otherwise, sort as strings
       return String(valA).localeCompare(String(valB));
   });


   // 渲染表格
   tableData.forEach(row => {
       var tr = document.createElement('tr');
       var dateCell = document.createElement('td');
       // Format date if it's a date object or ISO string
       let displayXAxis = row[xAxisCol];
        if (displayXAxis && typeof displayXAxis === 'string' && displayXAxis.endsWith('Z') && !isNaN(new Date(displayXAxis).getTime())) {
             displayXAxis = new Date(displayXAxis).toISOString().split('T')[0]; // Format as YYYY-MM-DD
        }
       dateCell.textContent = displayXAxis || '';
       tr.appendChild(dateCell);

       tableColumns.forEach(col => {
           var cell = document.createElement('td');
           var value = row[col] !== undefined ? row[col] : '';
           if (col.endsWith('率')) {
               value = Number(value) ? (Number(value) * 100).toFixed(2) + '%' : '0.00%';
           } else if (typeof value === 'number' && !isNaN(value)) {
               value = value.toLocaleString('en-US');
           } else {
               value = String(value);
           }
           cell.textContent = value;
           tr.appendChild(cell);
       });
       tableBody.appendChild(tr);
   });

   // 同步滚动
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
   var sheetData = allData[currentSheet];
   var siteIds = sheetData.site_ids;

   // 检查当前筛选值在新 sheet 中是否存在
   var filterToApply = siteIds.includes(currentFilterKey) ? currentFilterKey : (siteIds[0] || '汇总');

   // 更新筛选按钮
   var filterContainer = document.querySelector('.filter-container');
   filterContainer.innerHTML = siteIds.map(id =>
       // 添加 data-filter-key 属性以便查找按钮
       '<button data-filter-key="' + id + '" onclick="filterData(\\'' + id + '\\', this)">' + id + '</button>'
   ).join('');

   // 应用筛选并高亮对应的按钮
   filterData(filterToApply, null); // 传递 null，filterData 会自己找到并高亮按钮
}

function filterData(filterKey, element) {
   // 更新当前的筛选值
   currentFilterKey = filterKey;

   var sheetData = allData[currentSheet];
   var rawDf = JSON.parse(sheetData.raw_df);
   var xAxisCol = sheetData.x_axis_col;
   var filterCol = sheetData.filter_col;
   var filteredData = rawDf;

   if (filterKey !== '汇总') {
       filteredData = rawDf.filter(row => String(row[filterCol]) === String(filterKey));
   }

   // Group filtered data by the X-axis column
   var groupedData = {};
   // Initialize groupedData with all original labels and zero values
   sheetData.labels.forEach(label => {
       groupedData[label] = {};
       sheetData.datasets.forEach(dataset => {
           groupedData[label][dataset.label] = 0; // Initialize with 0
       });
   });

   // Populate groupedData with filtered data sums
   filteredData.forEach(row => {
       var xAxis = row[xAxisCol];
       // Ensure xAxis exists in groupedData (it should if using original labels)
       if (groupedData[xAxis]) {
           sheetData.datasets.forEach(dataset => {
               var value = row[dataset.label];
               // Handle potential null/undefined and ensure it's a number
               value = value !== undefined && value !== null ? parseFloat(value) || 0 : 0;
               groupedData[xAxis][dataset.label] = (groupedData[xAxis][dataset.label] || 0) + value;
           });
       }
   });

   // Construct the new data structure for Chart.js datasets ({x, y} objects)
   var newDataDatasets = [];
   sheetData.datasets.forEach(dataset => {
       let datasetDataPoints = [];
       // Iterate through the original labels to ensure all X-axis points are included
       sheetData.labels.forEach(label => {
           // Get the summed value for this label and dataset, default to 0
           let value = groupedData[label] ? groupedData[label][dataset.label] || 0 : 0;
           // Push the {x, y} object
           datasetDataPoints.push({ x: label, y: value });
       });
       newDataDatasets.push({
           label: dataset.label,
           data: datasetDataPoints, // This is the array of {x, y} objects
           backgroundColor: dataset.backgroundColor,
           borderColor: dataset.borderColor,
           borderWidth: dataset.borderWidth,
           type: dataset.type,
           yAxisID: dataset.yAxisID,
           fill: dataset.fill
       });
   });

   // Create the newData object structure expected by Chart.js update
   var newData = {
       labels: sheetData.labels, // Keep original labels for context, though category scale primarily uses data.x
       datasets: newDataDatasets
   };


   updateChart(currentSheet + '-chart', newData, filteredData);

   // 移除所有筛选按钮的 active 类
   var filterButtons = document.querySelectorAll('.filter-container button');
   filterButtons.forEach(btn => btn.classList.remove('active'));

   // 找到当前筛选值对应的按钮并添加 active 类
   // Use data-filter-key to find the button reliably
   var activeBtn = document.querySelector('.filter-container button[data-filter-key="' + filterKey + '"]');
   if (activeBtn) {
       activeBtn.classList.add('active');
   } else {
        // Fallback to highlight the first button if the filterKey button wasn't found
        var firstButton = document.querySelector('.filter-container button');
        if (firstButton) {
            firstButton.classList.add('active');
            currentFilterKey = firstButton.getAttribute('data-filter-key'); // Update filterKey
        }
   }
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

       if (rightGroup.items.length > 0 && canvasId !== '留存-chart') { // Exclude right legend for "留存"
           var rightGroupHtml = '<div class="legend-group"><h4>' + rightGroup.title + '</h4><div class="legend-items">';
           rightGroup.items.forEach(item => {
               rightGroupHtml += '<div class="legend-item"><div class="legend-color" style="background-color: ' + item.color + ';"></div>' + item.label + '</div>';
           });
           rightGroupHtml += '</div></div>';
           legendContainer.innerHTML += rightGroupHtml;
       }
        // Add left legend for "留存" even if right is excluded
        if (leftGroup.items.length > 0 && canvasId === '留存-chart' && rightGroup.items.length === 0) {
             var leftGroupHtml = '<div class="legend-group"><h4>图例</h4><div class="legend-items">'; // Simpler title for single group
             leftGroup.items.forEach(item => {
                 leftGroupHtml += '<div class="legend-item"><div class="legend-color" style="background-color: ' + item.color + ';"></div>' + item.label + '</div>';
             });
             leftGroupHtml += '</div></div>';
             legendContainer.innerHTML += leftGroupHtml;
        }


       // Clear previous legend before appending
       var existingLegend = chartContainer.querySelector('.chart-legend');
       if (existingLegend) {
           existingLegend.remove();
       }
       chartContainer.appendChild(legendContainer);
   }
   updateTable(newData, filteredData, allData[currentSheet].x_axis_col, allData[currentSheet].filter_col);
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
           [f'<li><a href="#" onclick="toggleDataView(\'{sheet}-container\', this)">{sheet}</a></li>' for sheet in sheets]
       )

       chart_containers = "\n".join(
           [f'''
<div id="{sheet}-container" class="chart-container">
   <h2>{sheet} 数据图</h2>
   <canvas id="{sheet}-chart"></canvas>
</div>''' for sheet in sheets]
       )

       # 初始加载时，筛选按钮由 JS 的 initCharts 和 toggleDataView 生成，这里可以留空或放一个占位符
       filter_buttons_placeholder = ""

       html = f"""
<!DOCTYPE html>
<html>
<head>
   <title>灰灰分析报告</title>
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
{filter_buttons_placeholder}
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
   with open("0_Ultimate_Analysis.html", "w", encoding="utf-8") as f:
       f.write(html_content)
   print("0_Ultimate_Analysis.html 文件已生成，包含所有功能和界面。")

   return html_content

if __name__ == "__main__":
   generate_0_Ultimate_Analysis_html()
