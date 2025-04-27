# 输入的会员 ID，按行分隔，没有标点符号
member_ids_input = """
tg708002
tg708001
"""

# 按行拆分输入，去掉空格，转换为小写，并格式化为 SQL 查询格式
member_ids_list = member_ids_input.strip().split('\n')
formatted_ids = ", ".join([f"'{id.replace(' ', '').lower()}'" for id in member_ids_list])

# 生成最终的 SQL 查询字符串
agent = f"AND a1_adm.agent_name IN ({formatted_ids})"
member = f"WHERE member_id IN ({formatted_ids})"

# 打印结果
print(agent)
print(member)

