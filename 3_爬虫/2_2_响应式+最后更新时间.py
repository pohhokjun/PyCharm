import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import whois

urls = """
chexun.com
jiakaobaodian.com
jiazhao.com
okeycar.com
chmotor.cn
zhongche.com
pingan.com
ghac.cn
ichezhan.com
audi.cn
ybjk.com
chinatruck.org
svw-volkswagen.com
cn-truck.com
cnev.cn
hx2car.com
dongfeng-honda.com
"""

# 将多行字符串分割成列表
urls = urls.strip().split("\n")


# 自动添加 URL 前缀的函数
def format_url(url):
    if not url.startswith(("http://", "https://")):
        return "http://" + url  # 默认加上 http://
    return url


# 检查网站是否响应式的函数
def check_responsive(url):
    formatted_url = format_url(url)  # 确保 URL 有前缀
    try:
        response = requests.get(formatted_url, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')

        # 查找<meta>标签，检查是否有viewport属性来判断是否为响应式设计
        if soup.find('meta', attrs={'name': 'viewport'}):
            return "响应式网站"
        else:
            return "非响应式网站"
    except Exception as e:
        return f"无法访问: {e}"


# 获取域名的最后更新时间
def get_domain_last_updated(url):
    try:
        domain = url.split("//")[-1].split("/")[0]  # 提取域名
        domain_info = whois.whois(domain)
        if isinstance(domain_info.updated_date, list):
            return domain_info.updated_date[0]  # 取第一个更新时间
        return domain_info.updated_date if domain_info.updated_date else "无更新时间"
    except Exception as e:
        return f"查询失败: {e}"


# 批量检查每个网址的响应式状态并导出结果
results = []
batch_size = 5  # 每次保存5条数据

output_file = "website_check_results.xlsx"

# 检查每个网址
for index, url in enumerate(urls):
    result = check_responsive(url)
    last_updated = get_domain_last_updated(url)
    results.append([url, result, last_updated])
    print(f"网址: {url}, 结果: {result}, 最后更新时间: {last_updated}")

    # 每处理完5条数据，就保存一次
    if (index + 1) % batch_size == 0 or (index + 1) == len(urls):
        # 创建 DataFrame
        df = pd.DataFrame(results, columns=["网址", "结果", "最后更新时间"])

        # 如果文件不存在，创建新文件并写入
        if not os.path.exists(output_file):
            df.to_excel(output_file, index=False)
        else:
            # 如果文件存在，使用 openpyxl 追加数据
            with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)

        print(f"已保存 {len(results)} 条数据到 {output_file}")

        # 重置结果列表，以便下一个批次使用
        results = []
