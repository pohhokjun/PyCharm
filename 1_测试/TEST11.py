import requests
import os
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time


class WebsiteDownloader:
    def __init__(self, url, save_dir="download", max_depth=5):
        # 如果URL没有协议，添加默认的https
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        self.base_url = url
        self.domain = urlparse(url).netloc
        self.save_dir = save_dir
        self.visited_urls = set()
        self.max_depth = max_depth  # 最大递归深度
        # 添加用户代理和请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # 创建保存目录
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

    def create_file_path(self, url):
        """根据URL生成保存路径，保持目录结构"""
        parsed = urlparse(url)
        path = parsed.path.strip('/')

        # 如果路径为空或以 / 结尾，保存为 index.html
        if not path:
            return os.path.join(self.save_dir, 'index.html')

        # 如果路径不以文件扩展名结尾，添加 index.html
        if not os.path.splitext(path)[1]:
            path = os.path.join(path, 'index.html')

        # 构造完整的保存路径
        full_path = os.path.join(self.save_dir, path)
        # 创建必要的目录
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        return full_path

    def download_file(self, url, path):
        """下载单个文件"""
        try:
            time.sleep(1)  # 添加1秒延迟，避免请求过快
            response = requests.get(url, stream=True, headers=self.headers, timeout=10)
            response.raise_for_status()

            # 创建必要的目录
            os.makedirs(os.path.dirname(path), exist_ok=True)

            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"已下载: {path}")
            return True
        except Exception as e:
            print(f"下载失败 {url}: {str(e)}")
            return False

    def download_page(self, url, depth=0):
        """递归下载页面及其资源"""
        if depth > self.max_depth:
            print(f"达到最大深度 {self.max_depth}，停止爬取: {url}")
            return

        if url in self.visited_urls or not url.startswith(self.base_url):
            return

        self.visited_urls.add(url)

        # 下载页面内容
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 保存HTML文件
            filepath = self.create_file_path(url)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"已保存页面: {filepath}")

            # 查找并下载资源（图片、CSS、JS、字体等）
            resource_tags = {
                'img': 'src',
                'link': 'href',
                'script': 'src',
                'source': 'src',  # 视频或音频的<source>标签
            }

            for tag, attr in resource_tags.items():
                for element in soup.find_all(tag):
                    resource_url = element.get(attr)
                    if resource_url:
                        absolute_url = urljoin(url, resource_url)
                        if urlparse(absolute_url).netloc == self.domain:
                            resource_path = self.create_file_path(absolute_url)
                            self.download_file(absolute_url, resource_path)

            # 下载其他文件类型（如PDF、视频等）通过<a>标签
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                absolute_url = urljoin(url, href)
                if urlparse(absolute_url).netloc == self.domain:
                    # 检查是否是文件类型（PDF、视频等）
                    if any(ext in href.lower() for ext in ['.pdf', '.mp4', '.mov', '.avi', '.woff', '.woff2', '.ttf']):
                        resource_path = self.create_file_path(absolute_url)
                        self.download_file(absolute_url, resource_path)
                    # 递归爬取子页面
                    elif absolute_url not in self.visited_urls and absolute_url.startswith(self.base_url):
                        self.download_page(absolute_url, depth + 1)

        except Exception as e:
            print(f"处理页面失败 {url}: {str(e)}")

    def start(self):
        """开始下载"""
        print(f"开始下载网站: {self.base_url}")
        self.download_page(self.base_url)
        print("下载完成！")


# 使用示例
if __name__ == "__main__":
    target_url = "https://www.newland.cn"
    downloader = WebsiteDownloader(target_url, max_depth=3)  # 设置最大深度为3
    downloader.start()