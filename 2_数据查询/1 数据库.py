import pymysql

try:
    # 建立数据库连接 (不指定数据库名称)
    connection = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='root',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    # 使用游标执行查询
    with connection.cursor() as cursor:
        # 查询所有数据库
        cursor.execute("SHOW DATABASES;")
        databases = cursor.fetchall()

        print("数据库列表:")
        for db in databases:
            print(db['Database'])

except pymysql.MySQLError as e:
    print(f"数据库错误: {e}")

finally:
    # 关闭连接
    if 'connection' in locals() and connection.open:
        connection.close()
        print("数据库连接已关闭")