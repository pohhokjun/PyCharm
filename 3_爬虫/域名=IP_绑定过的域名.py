import socket
import subprocess
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
from openpyxl import load_workbook

# 记录运行开始时间
start_time = time.strftime('%Y-%m-%d %H:%M')
print(f"运行开始时间: {start_time}")

# 配置日志
logging.basicConfig(
    filename='ip_domain_query.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8',
    filemode='w'
)

# **批量输入域名**
DOMAIN_INPUT = """
wmihealth.com
"""
domains = [d.strip() for d in DOMAIN_INPUT.split('\n') if d.strip()]

# 生成 Excel 文件名
excel_filename = f"IP查询_{time.strftime('%Y%m%d_%H%M')}.xlsx"


# **设置浏览器驱动**
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")

    service = Service()
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


# **全局共用一个 driver**
driver = setup_driver()


# **通过 Ping 获取 IP**
def get_ip_by_ping(domain):
    try:
        result = subprocess.run(
            ["ping", domain, "-n", "1"], capture_output=True, text=True, encoding='gbk'
        )
        for line in result.stdout.split("\n"):
            if "[" in line and "]" in line:
                ip = line.split("[")[1].split("]")[0]
                return ip
    except Exception as e:
        logging.error(f"Ping 获取 IP 失败 {domain}: {e}")
    return None


# **解析域名IP（优先 Ping 获取 IP，扩展前后 50 个）**
def resolve_domain_ip(domain):
    ip = None
    try:
        ip = socket.gethostbyname(domain)
    except socket.gaierror:
        logging.warning(f"{domain} 无法解析，尝试 Ping 获取 IP")
        ip = get_ip_by_ping(domain)

    if ip:
        base_parts = ip.split('.')
        if len(base_parts) == 4:
            base_prefix = ".".join(base_parts[:3])
            base_num = int(base_parts[3])
            expanded_ips = [f"{base_prefix}.{i}" for i in range(base_num - 50, base_num + 51) if 0 <= i <= 255]
        else:
            expanded_ips = [ip]
        logging.info(f"{domain} -> {expanded_ips}")
        return expanded_ips
    else:
        return ["解析失败"]


# **查询 IP 绑定的域名**
def query_ip_domains(ip):
    if ip == "解析失败":
        return ["无法查询: IP解析失败"]

    try:
        url = f"https://ipchaxun.com/{ip}"
        logging.info(f"开始查询: {url}")

        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        time.sleep(2)
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.date + a")))
        except Exception:
            logging.warning(f"{ip} 页面加载超时，未找到绑定域名")
            return ["未找到绑定域名"]

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        bound_domains = [entry.text.strip() for entry in soup.select("span.date + a")]

        logging.info(f"{ip} 绑定的域名: {bound_domains}")
        return bound_domains if bound_domains else ["未找到绑定域名"]

    except Exception as e:
        logging.error(f"查询失败 {ip}: {e}")
        return ["查询失败"]


# **保存到 Excel**
def save_to_excel(data, filename):
    try:
        df = pd.DataFrame(data)
        df = df.explode('绑定域名')  # 绑定域名一行一个

        try:
            with pd.ExcelWriter(filename, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)
        except FileNotFoundError:
            df.to_excel(filename, index=False, engine="openpyxl")

    except Exception as e:
        logging.error(f"保存 Excel 失败: {e}")


# **主程序**
results = []
for i, domain in enumerate(domains, start=1):
    logging.info(f"开始处理: {domain}")
    ips = resolve_domain_ip(domain)

    for ip in ips:
        bound_domains = query_ip_domains(ip) if ip != "解析失败" else ["无法查询: IP解析失败"]

        result = {
            "域名": domain,
            "IP": ip,
            "绑定域名": bound_domains
        }
        results.append(result)

        # **打印当前结果**
        print(f"已完成 {i}/{len(domains)}, IP: {ip}")
        print(f"  域名: {domain}")
        print(f"  IP: {ip}")
        print(f"  绑定域名: {', '.join(bound_domains)}")

        # **立即保存到 Excel**
        save_to_excel([result], excel_filename)
        print(f"已保存 Excel: {excel_filename}")

# **关闭全局 driver**
driver.quit()

# 记录运行结束时间
end_time = time.strftime('%Y-%m-%d %H:%M')
print(f"运行结束时间: {end_time}")
