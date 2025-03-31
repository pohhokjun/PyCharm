import os
import requests
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
import threading
from queue import Queue

# 随机 User-Agent，防止百度封禁
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]

THREAD_COUNT = 5  # 线程数（加快查询）


def get_baidu_rank(keyword, result_queue):
    """ 查询关键词在百度的排名 """
    url = f"https://www.baidu.com/s?wd={keyword}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        # 获取搜索结果
        results = soup.find_all("a", href=True)

        for index, result in enumerate(results, start=1):
            if "baidu.com" not in result["href"]:  # 过滤百度自家链接
                result_queue.put((keyword, index))
                return

    except Exception:
        pass

    result_queue.put((keyword, "未排名"))


def worker(keyword_queue, result_queue):
    """ 线程任务，从队列中获取关键词查询 """
    while not keyword_queue.empty():
        keyword = keyword_queue.get()
        get_baidu_rank(keyword, result_queue)
        keyword_queue.task_done()
        time.sleep(random.uniform(1, 3))  # 防止请求过快被封


def main():
    # 读取关键词文件
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    FILE_PATH = os.path.join(BASE_DIR, "keywords.py")

    with open(FILE_PATH, "r", encoding="utf-8") as f:
        keywords = [line.strip() for line in f if line.strip()]  # 读取每一行，去除空行

    keyword_queue = Queue()
    result_queue = Queue()

    for kw in keywords:
        keyword_queue.put(kw)

    threads = []
    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=worker, args=(keyword_queue, result_queue))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # 保存结果到 Excel
    result_df = pd.DataFrame(results, columns=["Keyword", "Baidu Rank"])
    result_df.to_excel("Baidu_Rank_Result.xlsx", index=False)

    print("✅ 百度排名查询完成，结果已保存到 'Baidu_Rank_Result.xlsx'")


if __name__ == "__main__":
    main()
