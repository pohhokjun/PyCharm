import os
import time
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# 批量输入学校名称
schools = """
枣庄职业学院
皖西卫生职业学院
皖江工学院
嘉兴南洋职业技术学院
浙江金华科贸职业技术学院
杭州师范大学钱江学院
浙江海洋大学
""".strip().split("\n")

# 获取当前时间（精确到分钟），格式化 Excel 文件名
current_time = datetime.now().strftime("%Y%m%d_%H%M")
excel_filename = f"词找网站+响应式_{current_time}.xlsx"

# 配置 Selenium
chrome_options = Options()
chrome_options.add_argument("--headless=new")  # 无头模式
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

# 启动 WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# 存储官网结果
data_list = []


# 解析百度跳转链接，获取真实 URL
def get_real_url(baidu_url):
    try:
        parsed_url = urlparse(baidu_url)
        if "baidu.com/link" in baidu_url:
            response = requests.get(baidu_url, allow_redirects=True, timeout=5)
            return response.url  # 获取最终跳转的 URL
        elif parsed_url.netloc:
            return baidu_url  # 直接返回真实域名的 URL
        else:
            return "解析失败"
    except requests.RequestException:
        return "解析失败"


# 遍历学校进行搜索
for idx, school in enumerate(schools, start=1):
    try:
        driver.get("https://www.baidu.com")
        search_box = driver.find_element(By.NAME, "wd")
        search_box.clear()
        search_box.send_keys(f"{school} 官网")
        search_box.send_keys(Keys.RETURN)
        time.sleep(2)  # 等待搜索结果加载

        # 获取第一个搜索结果
        results = driver.find_elements(By.CSS_SELECTOR, "h3 a")
        if results:
            baidu_url = results[0].get_attribute("href")
            real_url = get_real_url(baidu_url)  # 解析真实官网 URL
        else:
            real_url = "未找到官网"

    except Exception:
        real_url = "搜索异常"

    # 存入列表
    data_list.append([school, real_url])

    # **每 5 条数据写入一次 Excel**
    if len(data_list) % 5 == 0 or idx == len(schools):
        df = pd.DataFrame(data_list, columns=["学校", "官网"])

        # 写入 Excel
        if os.path.exists(excel_filename):
            with pd.ExcelWriter(excel_filename, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)
        else:
            df.to_excel(excel_filename, index=False)

        data_list.clear()  # 清空已写入的数据

# 关闭浏览器
driver.quit()

# **第二部分：检查学校网站是否响应式**
df = pd.read_excel(excel_filename)

# 自动添加 URL 前缀的函数
def format_url(url):
    if not isinstance(url, str):
        return None
    if not url.startswith(("http://", "https://")):
        return "http://" + url  # 默认加上 http://
    return url


# 检查网站是否响应式的函数
def check_responsive(url):
    formatted_url = format_url(url)  # 确保 URL 有前缀
    if not formatted_url:
        return "无效网址"

    try:
        response = requests.get(formatted_url, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')

        # 查找<meta>标签，检查是否有viewport属性来判断是否为响应式设计
        if soup.find('meta', attrs={'name': 'viewport'}):
            return "响应式网站"
        else:
            return "非响应式网站"
    except Exception:
        return "无法访问"


# 遍历已获取的官网，检查是否响应式
df["响应式检测"] = df["官网"].apply(check_responsive)

# 保存到 Excel
df.to_excel(excel_filename, index=False)
print(f"已更新 {excel_filename}，包含官网和响应式检测结果。")
