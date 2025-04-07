import requests
from bs4 import BeautifulSoup

def extract_brand_names(url):
    """
    从给定的URL的HTML内容中提取看起来像品牌词的数据。
    这个函数使用了一些简单的启发式方法，可能无法覆盖所有情况。

    Args:
        url (str): 要抓取的网页URL。

    Returns:
        list: 一个包含提取出的品牌词的列表。
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 如果请求失败，会抛出HTTPError异常
        soup = BeautifulSoup(response.content, 'html.parser')

        brand_names = set()

        # 尝试提取页面标题
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            # 可以根据标题的常见格式进行简单提取，例如去除 " - 豆瓣阅读" 后面的部分
            if " - " in title:
                brand_names.add(title.split(" - ")[0].strip())
            else:
                brand_names.add(title)

        # 查找可能包含品牌名称的常见元素，例如导航栏、页脚等
        for link in soup.find_all('a'):
            if link.string and len(link.string.strip()) > 1:
                brand_names.add(link.string.strip())

        for span in soup.find_all('span'):
            if span.string and len(span.string.strip()) > 1:
                brand_names.add(span.string.strip())

        for div in soup.find_all('div', class_=['site-nav', 'footer']): # 示例类名，可能需要根据实际网页调整
            if div.string and len(div.string.strip()) > 1:
                brand_names.add(div.string.strip())
            for a in div.find_all('a'):
                if a.string and len(a.string.strip()) > 1:
                    brand_names.add(a.string.strip())

        # 移除一些过于宽泛的词语，提高准确性 (可以根据实际情况调整)
        common_words_to_exclude = {'豆瓣', '阅读', '首页', '书店', '专栏', '电子书', '免费', '登录', '注册', '更多'}
        extracted_brands = [name for name in brand_names if name not in common_words_to_exclude and len(name) > 1]

        return list(set(extracted_brands)) # 去重后返回列表

    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        return []
    except Exception as e:
        print(f"解析HTML时发生错误: {e}")
        return []

if __name__ == "__main__":
    url = "https://read.douban.com/"
    brand_data = extract_brand_names(url)
    if brand_data:
        print("提取到的品牌词数据:")
        for brand in sorted(brand_data):
            print(brand)
    else:
        print("未能提取到品牌词数据。")