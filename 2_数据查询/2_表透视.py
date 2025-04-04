import pymysql

# 数据库配置
DATABASE_CONFIG = {
    'host': '18.178.159.230',
    'port': 3366,
    'user': 'bigdata',
    'password': 'uvb5SOSmLH8sCoSU',
    'database': 'finance_1000',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

try:
    # 配置项集中管理
    CONFIG = {
        'TABLE_NAME': 'finance_member_level',
        'TIME_FIELDS': ['created_at', 'updated_at'],
        'SELECT_FIELDS': [
            'site_id',
            'member_credit_level',
        ],
        'DISPLAY_MAPPINGS': {
            # 字段显示名称映射
            'FIELD_NAMES': {
                'member_name': '会员名称',
                'site_id': '站点 ID',
                'member_credit_level': '会员信用等级',
                'created_at': '创建时间',
                'updated_at': '更新时间'
            },
            # 字段值映射
            'FIELD_VALUES': {
                'lock_status': {
                    1: '启用',
                    2: '禁用'
                },
                'member_credit_level': {
                    1: '普通',
                    2: '白银',
                    3: '黄金',
                    4: '铂金'
                }
            }
        }
    }

    # 建立数据库连接
    connection = pymysql.connect(**DATABASE_CONFIG)

    # 使用游标执行查询
    with connection.cursor() as cursor:
        # 获取表结构（字段名和类型）
        cursor.execute(f"SHOW COLUMNS FROM {CONFIG['TABLE_NAME']}")
        columns_info = cursor.fetchall()
        all_columns = [col['Field'] for col in columns_info]
        column_types = {col['Field']: col['Type'] for col in columns_info}

        # 检测时间字段并查询 Min/Max
        time_columns = [col for col, col_type in column_types.items()
                        if 'datetime' in col_type or 'timestamp' in col_type or
                        (col_type.startswith('text') and col in CONFIG['TIME_FIELDS'])]
        if time_columns:
            print("\n时间字段的 Min 和 Max 值:")
            for col in time_columns:
                cursor.execute(f"SELECT MIN({col}), MAX({col}) FROM {CONFIG['TABLE_NAME']}")
                min_max = cursor.fetchone()
                print(f"{col}: Min = {min_max[f'MIN({col})']}, Max = {min_max[f'MAX({col})']}")
            # 添加总行数
            cursor.execute(f"SELECT COUNT(*) as total_rows FROM {CONFIG['TABLE_NAME']}")
            total_rows = cursor.fetchone()['total_rows']
            print(f"总行数: {total_rows}")

        # 使用字段列表构造查询
        query = f"""
        SELECT DISTINCT
            {', '.join(CONFIG['SELECT_FIELDS'])}
        FROM {CONFIG['TABLE_NAME']}
        """
        cursor.execute(query)
        distinct_results = cursor.fetchall()

        # 获取一行完整数据用于填充其他字段
        cursor.execute(f"SELECT * FROM {CONFIG['TABLE_NAME']} LIMIT 1")
        sample_row = cursor.fetchone() or {}

        if distinct_results:
            # 调整字段显示名称
            display_columns = [
                CONFIG['DISPLAY_MAPPINGS']['FIELD_NAMES'].get(col, col)
                for col in all_columns
            ]

            # 计算每列最大宽度
            col_widths = {col: max(len(display_columns[i]), len(column_types[col]))
                         for i, col in enumerate(all_columns)}
            for row in distinct_results:
                for col in CONFIG['SELECT_FIELDS']:
                    # 应用值映射
                    value = row[col]
                    if col in CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'] and value is not None:
                        value = CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'][col].get(value, value)
                    value = str(value) if value is not None else 'NULL'
                    col_widths[col] = max(col_widths[col], len(value))
            for col in all_columns:
                if col not in CONFIG['SELECT_FIELDS'] and col in sample_row:
                    value = sample_row[col]
                    if col in CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'] and value is not None:
                        value = CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'][col].get(value, value)
                    value = str(value) if value is not None else 'NULL'
                    col_widths[col] = max(col_widths[col], len(value))

            # 打印数据表格
            print("\n数据表格:")
            # 打印字段类型行
            type_row = " | ".join(f"{column_types[col]:<{col_widths[col]}}" for col in all_columns)
            print(type_row)
            # 打印字段名称行（使用显示名称）
            header = " | ".join(f"{display_columns[i]:<{col_widths[col]}}"
                              for i, col in enumerate(all_columns))
            print(header)
            print("-" * (len(header) + 2))

            for row in distinct_results:
                row_data = " | ".join(
                    f"{(CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'][col].get(row[col], row[col]) if col in row and row[col] is not None and col in CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'] else row[col] if col in row and row[col] is not None else CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'][col].get(sample_row.get(col), sample_row.get(col)) if col in sample_row and col in CONFIG['DISPLAY_MAPPINGS']['FIELD_VALUES'] and col not in CONFIG['SELECT_FIELDS'] else sample_row.get(col) if col in sample_row and col not in CONFIG['SELECT_FIELDS'] else 'NULL'):<{col_widths[col]}}"
                    for col in all_columns
                )
                print(row_data)
        else:
            print("\n表中没有数据或没有唯一组合")

except pymysql.Error as e:
    print(f"数据库错误: {e}")

finally:
    # 关闭连接
    if connection and connection.open:
        connection.close()
        print("数据库连接已关闭")