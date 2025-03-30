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

# 批量输入学校名称
schools = """
阿勒泰职业技术学院
新疆农业职业技术大学
昆明理工大学
玉溪职业技术学院
云南医药健康职业学院
云南锡业职业技术学院
云南工业信息职业学院
云南现代职业技术学院
云南能源职业技术学院
云南林业职业技术学院
云南科技信息职业学院
云南交通运输职业学院
云南经贸外事职业学院
云南工贸职业技术学院
香格里拉职业学院
文山学院
曲靖职业技术学院
怒江职业技术学院
昆明幼儿师范高等专科学校
昆明工业职业技术学院
大理农林职业技术学院
德宏职业学院
德宏师范学院
浙大城市学院
浙江体育职业技术学院
浙江特殊教育职业学院
浙江农业商贸职业学院
浙江工商职业技术学院
浙江东方职业技术学院
温州大学
温州科技职业学院
衢州职业技术学院
浙江金华科贸职业技术学院
杭州师范大学钱江学院
浙江海洋大学
呼伦贝尔学院
泰山护理职业学院
陕西交通职业技术学院
民办四川天一学院
""".strip().split("\n")

# 获取当前时间（精确到分钟），格式化 Excel 文件名
current_time = datetime.now().strftime("%Y%m%d_%H%M")
excel_filename = f"学校官网_{current_time}.xlsx"

# 配置 Selenium
chrome_options = Options()
chrome_options.add_argument("--headless")  # 无头模式，不打开浏览器
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

# 启动 WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# 存储结果
data_list = []


# 解析百度跳转链接，获取真实 URL
def get_real_url(baidu_url):
    try:
        response = requests.get(baidu_url, allow_redirects=True, timeout=5)
        return response.url  # 获取最终跳转的 URL
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

    except Exception as e:
        real_url = "搜索异常"

    # 存入列表
    data_list.append([school, real_url])

    # **每 5 条数据写入一次 Excel**
    if len(data_list) % 5 == 0 or idx == len(schools):
        df = pd.DataFrame(data_list, columns=["学校", "官网"])

        # 打印数据
        print("\n当前写入的数据：")
        print(df)

        # 写入 Excel
        if os.path.exists(excel_filename):
            with pd.ExcelWriter(excel_filename, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)
        else:
            df.to_excel(excel_filename, index=False)

        print(f"数据已写入 {excel_filename}")
        data_list.clear()  # 清空已写入的数据

# 关闭浏览器
driver.quit()
