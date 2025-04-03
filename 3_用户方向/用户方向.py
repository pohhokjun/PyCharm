import logging
import time
import random
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from tabulate import tabulate
from datetime import datetime
import tldextract
import re
import os
import asyncio

# 域名和关键词列表
domains = """
https://m.micro-maker.com/
http://www.aierxingz.com/
https://www.bet365.com/
https://1xbetind.in/
https://www.bet365.com/
https://www.ruimeidachuju.com/
"""

keywords = """
菠菜导航
博彩sports平台
"""

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("combined_uci_checker_playwright.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

ua = UserAgent()

async def get_playwright_browser():
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            f"--user-agent={ua.random}",
            "--window-size=1920,1080"
        ]
    )
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=ua.random,
        extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"}
    )
    await context.add_init_script(script="Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return playwright, browser, context

async def get_domain_data(page, domain):
    try:
        url = domain if domain.startswith(('http://', 'https://')) else f"https://{domain}"
        url = url.rstrip('/')
        await page.goto(url, timeout=15000)
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
        await asyncio.sleep(random.uniform(1, 3))
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        page_text = soup.get_text()
        content_length = len(page_text)
        link_count = min(len(re.findall(r'http[s]?://', page_text)), 50)
        logger.info(f"域名 {domain} - 内容长度: {content_length}, 外链数: {link_count}")
        return content_length, link_count
    except Exception as e:
        logger.warning(f"访问域名 {domain} 失败: {e}")
        return 0, 0

async def get_keyword_data(page, keyword, max_retries=3):
    for attempt in range(max_retries):
        try:
            await page.goto("https://m.baidu.com/", timeout=15000)
            await asyncio.sleep(random.uniform(2, 5))

            if "passport" in page.url or "login" in page.url.lower():
                logger.warning(f"第 {attempt + 1} 次尝试：检测到登录页面 {page.url}，重试...")
                await page.context.clear_cookies()
                await asyncio.sleep(random.uniform(3, 6))
                continue

            search_box = await page.wait_for_selector("#index-kw", timeout=10000)
            await search_box.fill("")
            for char in keyword:
                await search_box.type(char, delay=random.uniform(100, 300))
            await search_box.press("Enter")

            await page.wait_for_load_state("domcontentloaded", timeout=15000)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(random.uniform(2, 4))
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            related_keywords = []
            recommend_div = soup.find(lambda tag: tag.name == "div" and "大家还在搜" in tag.text)
            if recommend_div:
                related_keywords = [elem.text.strip() for elem in recommend_div.find_all("a") if elem.text.strip()]
            else:
                suggestion_divs = soup.find_all("div", class_="se-related-word")
                for div in suggestion_divs:
                    related_keywords.extend([elem.text.strip() for elem in div.find_all("a") if elem.text.strip()])
            related_keywords = [kw for kw in related_keywords if 2 <= len(kw) <= 20][:10]

            serp_links = [link for link in soup.find_all("a", href=True) if link.get("href").startswith("http")]
            top_10_links = serp_links[:10]
            unique_domains = set()
            total_content_length = 0
            total_link_count = 0
            known_domains_count = 0
            known_domains = ["baidu.com", "qq.com", "163.com", "sohu.com", "sina.com"]
            first_content_length = 0
            first_link_count = 0

            for idx, link in enumerate(top_10_links):
                try:
                    extracted = tldextract.extract(link["href"])
                    domain = f"{extracted.domain}.{extracted.suffix}"
                    unique_domains.add(domain)
                    await page.goto(link["href"], timeout=15000)
                    await page.wait_for_load_state("domcontentloaded")
                    link_html = await page.content()
                    link_soup = BeautifulSoup(link_html, "html.parser")
                    page_text = link_soup.get_text()
                    content_length = len(page_text)
                    total_content_length += content_length
                    link_count = min(len(re.findall(r'http[s]?://', page_text)), 50)
                    total_link_count += link_count
                    if domain in known_domains:
                        known_domains_count += 1
                    if idx == 0:
                        first_content_length = content_length
                        first_link_count = link_count
                except Exception as e:
                    logger.warning(f"访问 {link['href']} 失败: {e}")

            logger.info(
                f"关键词 {keyword} - 总内容长度: {total_content_length}, 总外链数: {total_link_count}, 知名域名数: {known_domains_count}, 独特域名数: {len(unique_domains)}, 第一名内容长度: {first_content_length}, 第一名外链数: {first_link_count}")
            return total_content_length, total_link_count, known_domains_count, related_keywords, len(unique_domains), first_content_length, first_link_count, top_10_links

        except PlaywrightTimeoutError:
            logger.error(f"关键词 {keyword} 第 {attempt + 1} 次尝试超时，重试...")
            await asyncio.sleep(random.uniform(3, 6))
        except Exception as e:
            logger.error(f"关键词 {keyword} 第 {attempt + 1} 次尝试失败: {e}")
            await asyncio.sleep(random.uniform(3, 6))

    logger.error(f"关键词 {keyword} 重试 {max_retries} 次后仍失败，跳过")
    return 0, 0, 0, [], 0, 0, 0, []

def calculate_uci(cf, lf, df, uf):
    uci = 0.4 * cf + 0.3 * lf + 0.2 * df + 0.1 * uf
    return min(100, round(uci, 2))

async def process_keyword(page, keyword):
    total_content_length, total_link_count, known_domains_count, related_keywords, ucs, first_content_length, first_link_count, top_10_links = await get_keyword_data(page, keyword)
    avg_content_length = total_content_length / 10 if total_content_length else 0
    avg_link_count = total_link_count / 10 if total_link_count else 0

    cf = (avg_content_length / 10000) * 80 + (first_content_length / 10000) * 20
    cf = min(100, cf)
    lf = (avg_link_count / 50) * 80 + (first_link_count / 50) * 20
    lf = min(100, lf)
    df = (known_domains_count / 10) * 80 + (20 if top_10_links and "baidu.com" in tldextract.extract(top_10_links[0]["href"]).domain else 0)
    df = min(100, df)
    uf = (ucs / 10) * 100

    uci = calculate_uci(cf, lf, df, uf)
    logger.info(f"关键词 {keyword} - CF: {cf}, LF: {lf}, DF: {df}, UF: {uf}, UCI: {uci}")

    first_url = top_10_links[0]["href"] if top_10_links else ""
    return {
        "屏蔽词": keyword if not related_keywords else "",
        "关键词": keyword,
        "UCI": uci,
        "1级": ";".join(related_keywords),
        "排名第一的网址": first_url
    }

async def process_domain(page, domain):
    content_length, link_count = await get_domain_data(page, domain)
    cf = min(100, (content_length / 10000) * 100)
    lf = min(100, (link_count / 50) * 100)
    df = 100 if domain in ["baidu.com", "qq.com", "163.com", "sohu.com", "sina.com"] else 0
    uf = 50
    uci = calculate_uci(cf, lf, df, uf)
    logger.info(f"域名 {domain} - CF: {cf}, LF: {lf}, DF: {df}, UF: {uf}, UCI: {uci}")
    return {
        "域名": domain,
        "UCI": uci
    }  # Removed "排名第一的网址"

def save_to_excel(filename, keyword_results, domain_results, append=True):
    try:
        if not append or not os.path.exists(filename):
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                if keyword_results:
                    pd.DataFrame(keyword_results).to_excel(writer, sheet_name='Sheet1', index=False)
                if domain_results:
                    pd.DataFrame(domain_results).to_excel(writer, sheet_name='Sheet2', index=False)
        else:
            with pd.ExcelWriter(filename, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                if keyword_results:
                    existing_keywords = pd.read_excel(filename, sheet_name='Sheet1') if 'Sheet1' in pd.ExcelFile(filename).sheet_names else pd.DataFrame()
                    df_keywords = pd.DataFrame(keyword_results)
                    updated_keywords = pd.concat([existing_keywords, df_keywords]).drop_duplicates(subset=['关键词'], keep='last')
                    updated_keywords.to_excel(writer, sheet_name='Sheet1', index=False)
                if domain_results:
                    existing_domains = pd.read_excel(filename, sheet_name='Sheet2') if 'Sheet2' in pd.ExcelFile(filename).sheet_names else pd.DataFrame()
                    df_domains = pd.DataFrame(domain_results)
                    updated_domains = pd.concat([existing_domains, df_domains]).drop_duplicates(subset=['域名'], keep='last')
                    updated_domains.to_excel(writer, sheet_name='Sheet2', index=False)
        logger.info(f"结果已保存/追加到 {filename}")
    except Exception as e:
        logger.error(f"保存 Excel 失败: {e}")

async def main():
    logger.info("程序启动")
    playwright, browser, context = None, None, None

    try:
        playwright, browser, context = await get_playwright_browser()
        page = await context.new_page()
        logger.info("浏览器初始化完成")

        domain_list = [line.strip().rstrip('/') for line in domains.splitlines() if line.strip()]
        keyword_list = [line.strip() for line in keywords.splitlines() if line.strip()]

        if not domain_list or not keyword_list:
            logger.error("域名或关键词列表为空，程序退出")
            return

        current_time = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"combined_uci_{current_time}.xlsx"

        keyword_results = []
        domain_results = []
        batch_size = 5

        for i, keyword in enumerate(keyword_list):
            logger.info(f"正在处理关键词 {keyword}")
            result = await process_keyword(page, keyword)
            keyword_results.append(result)
            if (i + 1) % batch_size == 0 or (i + 1) == len(keyword_list):
                save_to_excel(filename, keyword_results, [], append=(i >= batch_size))
                keyword_results = []
                logger.info(f"已处理 {i + 1} 个关键词，保存批次")
            await asyncio.sleep(random.uniform(3, 6))

        for i, domain in enumerate(domain_list):
            logger.info(f"正在处理域名 {domain}")
            result = await process_domain(page, domain)
            domain_results.append(result)
            if (i + 1) % batch_size == 0 or (i + 1) == len(domain_list):
                save_to_excel(filename, [], domain_results, append=True)
                domain_results = []
                logger.info(f"已处理 {i + 1} 个域名，保存批次")
            await asyncio.sleep(random.uniform(3, 6))

        if os.path.exists(filename):
            print("\n关键词 UCI (Sheet1):")
            df_keywords = pd.read_excel(filename, sheet_name='Sheet1')
            print(tabulate(df_keywords, headers="keys", tablefmt="grid", showindex=False))
            print("\n域名 UCI (Sheet2):")
            df_domains = pd.read_excel(filename, sheet_name='Sheet2')
            print(tabulate(df_domains, headers="keys", tablefmt="grid", showindex=False))

    except Exception as e:
        logger.error(f"程序运行出错: {e}")
        if keyword_results or domain_results:
            save_to_excel(filename, keyword_results, domain_results, append=True)
        raise
    finally:
        if context:
            await context.close()
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
        logger.info("浏览器已关闭")
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())