import asyncio
import os
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright
import aiofiles
import aiohttp

async def download_file(session, url, filepath):
    """异步下载文件"""
    try:
        async with session.get(url) as response:
            if response.status == 200:
                # 检查文件是否已存在，如果存在则跳过
                if not os.path.exists(filepath):
                    async with aiofiles.open(filepath, 'wb') as f:
                        await f.write(await response.read())
                    print(f"Downloaded: {url} -> {filepath}")
                else:
                    print(f"File already exists: {filepath}")
            else:
                print(f"Failed to download: {url} (status: {response.status})")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

async def download_website(url, output_dir):
    """使用 Playwright 下载整个网站，包括动态内容"""
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # 确保 url 是正确的完整格式
        if not url.startswith("http"):
            url = "https://" + url

        await page.goto(url, wait_until="networkidle")  # 等待网络空闲

        visited = set()
        queue = [url]
        async with aiohttp.ClientSession() as session:
            while queue:
                current_url = queue.pop(0)
                if current_url in visited:
                    continue
                visited.add(current_url)

                try:
                    await page.goto(current_url, wait_until="networkidle")  # 等待网络空闲
                    content = await page.content()

                    # 保存 HTML 文件
                    filepath = Path(output_dir) / urlparse(current_url).netloc / urlparse(current_url).path.strip('/') / 'index.html'
                    filepath.parent.mkdir(parents=True, exist_ok=True)
                    # 检查文件是否已存在，如果存在则跳过
                    if not os.path.exists(filepath):
                        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                            await f.write(content)
                    else:
                        print(f"File already exists: {filepath}")

                    # 提取链接和资源（包括动态生成的）
                    links = await page.evaluate('''() => {
                        const links = [];
                        const elements = document.querySelectorAll('a, link, img, script, source');
                        elements.forEach(element => {
                            if (element.href) links.push(element.href);
                            if (element.src) links.push(element.src);
                        });
                        return links;
                    }''')

                    for link in links:
                        absolute_url = urljoin(current_url, link)
                        if urlparse(absolute_url).netloc == urlparse(url).netloc:
                            if absolute_url not in visited:
                                queue.append(absolute_url)

                            # 下载资源文件
                            resource_path = Path(output_dir) / urlparse(absolute_url).netloc / urlparse(absolute_url).path.strip('/')
                            resource_path.parent.mkdir(parents=True, exist_ok=True)
                            await download_file(session, absolute_url, resource_path)

                except Exception as e:
                    print(f"Error processing {current_url}: {e}")

        await browser.close()

# 示例用法
website_url = "www.newland.cn"
output_directory = "downloaded_website"
asyncio.run(download_website(website_url, output_directory))