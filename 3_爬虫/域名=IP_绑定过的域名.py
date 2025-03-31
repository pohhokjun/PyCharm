import socket
import logging
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# 记录运行开始时间
start_time = time.strftime('%Y-%m-%d %H:%M')
print(f"运行开始时间: {start_time}")

# 配置日志
logging.basicConfig(
    filename='ip_domain_query.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# 域名输入
DOMAIN_INPUT = """
https://www.lngqjt.com/
https://www.185hcq.com/
https://www.hty888888.com/
https://www.58mingjiekeji.com/
http://www.dlyrl.com/
https://www.beitejinmen.com/
https://www.hiiav.net/
https://m.clean1995.com/
https://m.jingaohua.com/
http://www.sunyeon.cn/
https://m.js-ac.com/
https://www.hcxev.com/
https://www.mtkry.com/
https://www.lf-lt.com/
"""
domains = [d.strip() for d in DOMAIN_INPUT.split('\n') if d.strip()]

# **去掉 http:// 和 https://**
domains = [d.replace("http://", "").replace("https://", "").strip("/") for d in domains]

# 生成 Excel 文件名
excel_filename = f"IP查询_{time.strftime('%Y%m%d_%H%M')}.xlsx"


# 设置浏览器驱动
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")

    service = Service()  # 使用默认 ChromeDriver
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


# **全局共用一个 driver**
driver = setup_driver()


# 解析域名IP
def resolve_domain_ip(domain):
    try:
        ip = socket.gethostbyname(domain)
        logging.info(f"{domain} -> {ip}")
        return ip
    except socket.gaierror as e:
        logging.error(f"IP解析失败 {domain}: {e}")
        return "解析失败"


# 查询IP绑定的域名
def query_ip_domains(ip):
    if ip == "解析失败":
        return ["无法查询: IP解析失败"]

    try:
        url = f"https://ipchaxun.com/{ip}"
        logging.info(f"开始查询: {url}")

        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # 等待数据加载（最多 15 秒）
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.date + a")))
        except Exception:
            logging.warning(f"{ip} 页面加载超时，未找到绑定域名")
            return ["未找到绑定域名"]

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # **精准提取绑定域名**
        bound_domains = [entry.text.strip() for entry in soup.select("span.date + a")]

        logging.info(f"{ip} 绑定的域名: {bound_domains}")
        return bound_domains if bound_domains else ["未找到绑定域名"]

    except Exception as e:
        logging.error(f"查询失败 {ip}: {e}")
        return [f"查询失败: {e}"]


# **主程序**
results = []
for i, domain in enumerate(domains, start=1):
    logging.info(f"开始处理: {domain}")
    ip = resolve_domain_ip(domain)
    bound_domains = query_ip_domains(ip) if ip != "解析失败" else ["无法查询: IP解析失败"]

    result = {
        "域名": domain,
        "IP": ip,
        "绑定域名": ", ".join(bound_domains)
    }
    results.append(result)

    # **每完成 5 个，就 print 并保存 Excel**
    if i % 5 == 0 or i == len(domains):
        print(f"已完成 {i}/{len(domains)}")

        # **打印当前 5 个的结果**
        for res in results[-5:]:  # 只打印最新 5 个
            print(f"{res['域名']}:")
            print(f"  IP: {res['IP']}")
            print(f"  绑定域名: {res['绑定域名']}")

        # **追加写入 Excel**
        df = pd.DataFrame(results)
        df.to_excel(excel_filename, index=False, engine='openpyxl')
        print(f"已保存 Excel: {excel_filename}")

# **关闭全局 driver**
driver.quit()

# 记录运行结束时间
end_time = time.strftime('%Y-%m-%d %H:%M')
print(f"运行结束时间: {end_time}")
