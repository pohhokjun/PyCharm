import asyncio
import logging
import re
import time
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from openpyxl.styles import Alignment
from openpyxl import load_workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE  # 导入 ILLEGAL_CHARACTERS_RE
import os
import tldextract
import json

# 关键词和域名列表
KEYWORDS = '''
亚傅体育app入口登录
od体育app最新版下载
彩博汇app登录入口
台球竞彩官网app入口
爱竞彩APP官方网站
188金宝搏博亚洲体育
亚博一万多流水反水多少
全球赌彩公司排名
体育搏彩网站官网
彩博888app下载官网
拼搏在线福彩试机号码
十大澳门网投平台信誉排行榜
捕鱼大咖高爆版链接入口
捕鱼赌钱
一比一兑现的捕鱼
乐乐捕鱼安装
捕鱼大咖官网首页入口
赌彩网app下载
十大正规彩票网站
千锦娱乐1000亿彩票app官网下载
博大彩票app官方下载
网上赌彩票平台有哪些
彩博汇网官方网站
十大正规网络购彩平台
q彩网官方网站入口
中国彩票竞彩网
久久发娱乐彩票998
亚洲彩票最信赖的平台
大众彩票app下载有返水
玩彩网com380
近期网上彩票诈骗案
电竞竞猜平台app
电竞投注app官网
电竞赛事押注平台正规
澳门PG电子官网版
新人下载注册送68彩金
博采网络股吧
pg电子娱乐平台下载1号
亚洲彩票登录平台
手机上玩快三输十万能追回
电子游戏试玩平台
网上购彩平台app下载
十大网赌网址大全
澳门官方版棋牌娱乐平台
线上棋牌平台网
棋牌新人注册送10元
直接棋牌
168棋牌www官方网址手机版
十大棋牌软件排行榜
威斯尼斯ww8888棋牌
赢钱平台
好博体育app下载
体育彩票买球算赌博吗
网赌刷流水薅羊毛方法
十大正规买足球网站
亚傅体育app下载安装
y31成色1.23ct
菲律宾十大网赌官网
赌粉在哪个平台引流
网赌怎么推广拉客户
99499www威尼斯游戏背景
赌博sports娱乐
熊猫体育app官方入口
赌博sports中国
10～20元棋牌可提
赌博sports网址
赌博sports集团
赌博sports平台
网投十大信誉可靠平台
十大网投正规信誉官网
赌博sports安全可靠
赌博sports唯一
赌博sports全站
赌博sports网站
免费送2000试玩金
赌博线上官网
亚博yabo888vip官网最新版
赌钱捕鱼
捕鱼大作战3d官方网站
趣玩捕鱼官网下载
正规网赌博app下载
网赌彩票app
澳门赌彩网app官方网站
玩彩票输了几十万可以追回吗
十大网赌正规网址下载
十大信誉赌博官网
赌博电竞网页版
网赌app下载
亚洲赌场彩票app下载
赌场彩票返水最高是多少
彩票上瘾算赌徒吗
98彩官网登录入口
电竞押注的正规平台
电竞返水最高怎么处罚的
网赌十大软件app
威斯尼斯WW708mc棋牌
pg娱乐电子游戏app
pg电子娱乐官网下载
赌博电子官网
网赌戒不掉.一有钱就赌
赌注app
澳门网赌app下载
网赌流水10万拘留多少天
电子赌博模拟器
网上参赌怎么判刑
赌徒专用导航
棋牌电子游戏软件平台
网赌棋牌排行榜
可以上分的棋牌平台
10元20元就能充值的棋牌
安博体育app官网入口
网赌小游戏app
云开体育app官网登录入口
新人注册就送49元彩金
体育赌博app下载
体育下赌注app
体育赌博登录官网
赌场体育返水最高纪录是哪一年
赌场体育登录平台
斗牛赢钱真人版下载
网上赌最有保障的平台
ww.86开元棋牌
赢钱游戏一天赚200
388vip棋牌官网版
个人参与网赌怎么定罪
澳门赌牌网页游戏真人版
澳门线上真人发牌
网赌的七种类型
赌博最科学的投注法
赌博体育返水最高
菠菜健身管理系统
菠菜健身系统登录平台
菠菜sports中国
菠菜sports平台
菠菜sports娱乐
菠菜sports主页
菠菜sports官网
菠菜推币机app
亚洲菠菜十大平台推荐
菠菜套利论坛交流
菠菜网免费领取体验金
菠菜sports在线
波克城市捕鱼
捕鱼app排行榜前十名
彩金捕鱼
波克捕鱼官方最新版下载
波克捕鱼改名叫什么了
乐乐捕鱼
菠菜捕鱼入口
波克捕鱼官方正版下载中心
波克捕鱼官方版
菠菜捕鱼平台
菠菜捕鱼官网入口
菠菜捕鱼登录
菠菜捕鱼返水最快方法
波克捕鱼官方版最新版
菠菜导航网
菠菜彩票官网
菠菜彩票入口
菠萝网彩票app下载地址
菠菜网最新平台网
菠菜彩票官网登录入口
菠菜彩票网app下载安装最新版本更新内容
菠菜彩票网官网登录入口
竞彩网官网app
菠菜彩票返水最简单三个步骤
菠菜彩票app是正规彩票吗
中国彩票网菠菜app
菠菜网免费领取彩金
菠菜试玩官网app
菠菜彩票APP登录入口
菠菜彩票平台app下载
专业菠菜导航官网
电竞菠菜app哪个好点
菠菜电竞亚洲官网入口功能介绍
绝地求生竞技
十大菠菜网正规平台
菠菜系统登录入口
菠菜电子app官方下载
菠菜电子官网登录入口
菠菜导航首页登录入口
菠菜电子app官方入口
菠菜电子站网页版登录入口
电子菠菜
菠菜电子地址怎么查出来最新消息
全球十大菠菜公司
国际菠菜规则
2020电子菠菜大全
菠菜电子浏览器官网入口
菠菜小说网手机版
菠菜电子娱乐登录入口今日更新
亚洲十大菠菜网
菠菜套利新方法
用银行卡跑菠菜料安全吗
菠菜电子app官网
菠菜电子网页版入口
菠菜电子平台官网
十大菠菜信誉网导航
菠菜棋牌平台官网入口
菠菜棋牌娱乐
芒果棋牌
菠菜棋牌官方最新版本更新内容介绍
菠菜棋牌官网入口
菠菜棋牌正版最新版本更新内容介绍
菠菜棋牌
菠菜棋牌官网进入
能打德州的棋牌软件
菠菜棋牌亚洲版最新版本亮点
可以反水的棋牌平台
最火棋牌排行榜前十名
菠菜棋牌中国官方入口2023年最新版
菠菜棋牌正版官网
菠菜棋牌平台最新版本更新内容介绍
菠菜棋牌改名后叫什么
菠菜棋牌官方正版入口
菠菜棋牌官网首页
菠菜体育app登录入口
菠菜体育下载入口官网
菠菜体育官网入口
菠菜体育
什么软件可以买足球
竞猜体育彩票首页
菠菜娱乐app官网入口
菠菜体育返水最简单三个步骤
免费看球赛的软件
beat365亚洲体育在线官网
菠菜真人娱乐
菠菜真人真人2024年最新电视剧预告
菠菜真人集团董事长简历
菠菜真人国际官网登录入口
菠菜真人主页的登录方式和历史背景是什么
菠菜真人竞猜官网入口
菠菜真人直播投注官网
亚洲十大菠菜排行
菠菜真人在线观看
菠菜真人无删减版观看
菠菜真人app安装下载
bet356亚洲版体育官网登录入口
亚傅体育网页版登录入口找不到了吗
sportsbet官网下载
博乐体育app官方入口
国外竞猜网站都有哪些
网赌刷返水五年
BOB博鱼综合游戏官网
澳彩app官网入口
体育在线投注365官网
好彩捕鱼高爆版
好彩捕鱼官方下载
博鱼体彩APP官方网站
澳门彩官方app下载
澳门官方彩票网
十大搏彩公司平台
手机版彩票app下载
所有彩票app平台下载
竞彩足彩官方网站
亚洲彩票登陆平台
BB彩票平台APP
可以试玩彩票的平台
下载彩票平台app下载
网上彩票诈骗的流程
电竞投注官网入口
亿博电竞平台登录入口
亚洲必赢电子游戏官网登陆
JBO竞博入口
澳门买彩票网站www
鑫博国际彩票官网下载
welcome购彩大厅登入
博亚彩票官网下载
PG电子娱乐官网
电子化彩票
鸿博国际网彩APP
必送26元彩金app下载
亚博体彩手机版官网
网赌电子游戏刷流水的办法
电子官方下载赌博
棋牌牛牛10元起充
博澳体育app官网入口下载
亚傅体育全站app下载
中国足彩网竞彩官网
HB火博·体育(中国)官方网站
体育对刷套利赚千万
真人彩票平台官方网站
澳门真人百家家乐app
真人竞技网赌app下载
AG真人国际官方网站
竞彩足球500app下载
C7赌博官方网站官网
赌博sports综合
赌博sports手机版
赌博sports官网
游客登录免费送2000试玩金
赌博app导航
捕鱼微信10元起上10元下
24h微信上下分捕鱼
捕鱼一晚上赢了12万
变态捕鱼
捕鱼大作战官方网
经典捕鱼下载
亚洲捕鱼app下载
打鱼赢现金提现到微信24小时
乐乐捕鱼微信登录版本
网赌app下载官网最新版
彩票其实早就内定了
500彩票手机版app下载
十大正规彩票app排行
赌博电竞主页
微信买体彩竞彩算赌博吗
娱乐彩票app下载
赌场彩票返水最高纪录
靠谱的十大彩票平台
凤凰娱乐彩票平台登录
彩票导航路线入口
电竞赌博app在线下载
赌博电竞官网
电竞比赛押注平台app
电竞赌博app有哪些
c7pg电子娱乐平台下载
赌电竞输了几十万
lol比赛押注平台入口
电竞返水最高纪录
注册免费送彩金100
电子游戏网赌大全
正规官方电子游戏app
PG电子娱乐平台
网赌电子游戏50块都赢不了
赌博电子app
9499www威尼斯
澳门娱乐场app网站大全
开元棋下载app官方版
999棋牌娱乐网站
赌场返水最高处罚标准2023年
最靠谱的十大棋牌网站
Bet365亚洲官方网站
足球赌注网站
明陞·体育官网app
九博体育app下载官网
网赌游戏软件开发app安装
澳门娱乐场app下载安装最新版本
赌场体育返水最高纪录2023年11月
赌博体育网页版
网赌是怎样控制让你输的
百家乐打闲傻瓜打法会赢钱吗
真人斗地主
赌博棋牌网站
成色18k1835mb
赌博真人中国
网赌真人版直接进入
赌博棋牌主页
一个成功赌徒的注码法
赌博棋牌返水最高
网赌是不是和真人在玩
赌博真人下载
菠菜sports免费
菠菜sports全站
菠菜sports集团
菠菜sports手机版
菠菜sports最新版本更新内容分享
菠菜sports下载
菠菜捕鱼网页版
捕鱼上下分100元10000
波克新仙魔九界捕鱼下载官网
官方专区波克捕鱼
捕鱼娱乐app官网下载
菠菜捕鱼返水最简单三个步骤
菠菜彩票登录
菠菜送彩金网址大全
菠萝彩彩票登录入口
菠菜国际平台app下载
菠萝彩论坛官方网站首页
菠菜彩票app官方下载
彩虹多多彩票app官网
菠菜彩票网(官网)
lol竞猜菠菜网
菠菜娱乐
亚洲彩票welcome官方网站
菠菜彩票的返水规则详解
菠菜彩票是正规平台吗
菠菜试玩福彩3d
菠菜彩票app下载安装最新版本使用方法
菠菜彩票官网首页
菠菜彩票app官方入口
菠菜电竞官网进入
菠菜电竞官网入口
菠菜电竞主页怎么没了
dota2菠菜竞猜平台
电竞娱乐
菠菜电子登录入口最新版本更新内容
菠菜电子APP下载官网
菠菜电子版官网入口
菠菜电子官网登录
靠谱的菠菜网
菠菜的电子地址在哪里啊
菠菜电子综合app官方下载最新版本更新内容介绍
菠菜电子集团董事长王东简介
靠谱的体育菠菜APP
菠菜电子书官网入口
亚洲有名的菠菜
菠菜投注网
菠菜什么含量最高
菠菜什么时候传入中国的
菠菜电子官网
菠菜棋牌官网登录入口
菠菜棋牌2024官方版最新版本特色
菠菜棋牌(老版本)
菠菜棋牌娱乐最新版本更新内容分享时间
菠菜棋牌网页版入口
菠菜棋牌登录入口2023最新版
菠菜棋牌娱乐官网
菠菜体育app官方下载
菠菜体育app官方入口
菠菜体育app官网
菠菜体育网页版入口
菠菜返水最高40天还能吃吗
菠菜真人账号注册入口
菠菜真人登录入口2024年最新版
菠菜真人接口
菠菜真人官网入口
菠菜真人网页版登录入口
真人菠菜现金
菠菜真人集团董事长是谁
菠菜真人国际app的最新版本更新内容和特色功能介绍
菠菜真人游戏官网入口
菠菜真人竞猜是什么软件
菠菜技术交流论坛
菠菜真人安全可靠app最新版本更新内容
菠菜真人视频大全
菠菜真人免费观看入口
菠菜真人登陆入口官网最新版本更新内容
云开体育app下载安装
888sport体育app下载
好博体育app官网
188博金宝(亚洲)体育官方网站
万博max体育官网入口
BET365亚洲唯一官网
投注返水什么意思
sportsbet赛博
世博体育app官网入口
拼搏在线彩神通官网手机版
bsports登录入口app官方版
博万体育app
博鱼·体育官网app
博乐彩票app下载
168官方彩票版app下载
目前国家允许的购彩平台
彩票app十大排名
竞彩足球开奖结果
娱乐彩票官网app下载
玩彩网app官网
千亿彩网app彩票
线上彩票店铺app
JBO竞博电竞APP下载
博电竞app下载
竞彩网官网平台
中国竞彩足彩官网首页
彩下载彩博汇
电子娱乐平台app下载
亚洲彩票登录app首页
全球十大搏彩
博乐彩票是不是正规的
合法网上购彩平台
手机购彩大厅登录首页
电子线上网站
菠菜导航网信誉平台
牛牛棋牌官方最新版
彩票娱乐平台线上
网赌对刷最怕三个东西
好博体育app下载安装
体育竞彩推荐平台app
皇冠新体育app官方入口下载
足球打水套利一天2000
亚傅体育app官网下载
真人体育彩票娱乐平台
澳博app下载官网
竞彩足球 投注
赌博sports亚洲
赌博sports免费
赌博sports安装
赌注游戏
开发一款网赌app需要多少钱
赌博sports登录
试玩软件app赚钱平台
澳门娱乐app官网入口网址
正规网赌十大排行网址
可以下分的捕鱼app
皇家捕鱼电玩城官网
捕鱼大作战官方网站入口
24小时在线捕鱼平台
捕鱼大世界官网入口
娱乐捕鱼
最新款捕鱼手游
赌鱼地址大全一览表
一天挣5000块钱捕鱼
捕鱼游戏网站平台大全
捕鱼app大全下载
手机捕鱼输了62万
赌博打鱼游戏平台
充钱真人捕鱼app
什么捕鱼游戏爆率最高
可以兑换人民币的捕鱼游戏
在线游戏捕鱼
959cc娱乐彩票
十大正规网赌app
2025彩票app下载
pg赌博软件下载
赌彩综合版
赌博棋牌网站大平台
106官方彩票app下载
今日竞彩足球胜平负
赌场彩票返水最高是6还是7
十大正规靠谱的彩票app
800cc彩票平台
https://game.87game88.com
电竞娱乐app官方网站入口
澳门电子游戏网站最新
买电竞比赛输赢的app
导航到网鱼电竞
网赌送38彩金
注册送注册金的电子平台
赌博电子娱乐
赌博电子平台
永远不会输的4倍投方法
网赌电子流水最高是3万还是5万
网站赌博电子游戏背后真相
pg电子免费送试玩金
送2000试玩金的平台
pg电子麻将胡了最新版本更新内容
赌博电子网页版
赌博电子主页
返水最高处罚标准2024年最新消息及处罚方法
可以提现的打牌平台
BW博万体育手机网页入口
利博体育平台登录入口
ng体育官网app下载
赌博体育官网
赌博体育在线
体育娱乐app下载
赌博体育全站
赌博体育网址
亚洲体育娱乐平台
体育投注官网下载
2023年赌场体育返水新规定及处罚详解
澳门特马网站.www
开设赌场罪最轻的三个阶段
麻将真人版下载
赌博真人唯一
赌博体育竞猜
返水最多的平台
赌博真人官方
赌博真人官网
菠菜sports登录
菠菜sports官方下载
菠菜sports最新
菠菜sports平台最新版本更新内容
正规的菠菜app
安博体育app官网登录入口
菠菜捕鱼官网
10000炮捕鱼官方下载
电玩捕鱼
菠菜捕鱼app官方下载
亚洲菠菜APP下载最新版本更新内容
菠菜捕鱼返水规律
捕鱼直播
菠菜彩票首页
菠菜彩票集团倒闭了吗
菠菜彩票app下载安装最新版本2023
polocai官网登录入口
菠菜彩票网app下载最新版本更新内容介绍
竞彩网首页
菠菜彩票怎么返水最厉害
中国彩票官网
菠菜试玩app官方下载
菠菜系统登录
菠菜彩票官网首页入口
菠菜电竞官方下载app最新版本更新内容
菠菜电竞app官方入口
菠菜电竞(网页版)在线观看
雷火电竞app官网入口
菠菜电竞主页最新消息今天视频播放
菠菜电竞平台app下载
菠菜电竞改名后叫啥
菠菜电竞导航手机版最新版本更新内容
菠菜电子商城官网
菠菜电子官网入口
菠菜电子APP下载
菠菜电子书官网
菠菜彩金白菜网
菠菜电子查询
菠菜综合官网
菠菜公司十大平台网
菠菜电子平台登录入口
菠菜电子娱乐APP下载安装
亚洲bocai
菠菜什么含量高
菠菜传入中国
菠菜电子官方版下载安装
菠菜电子网页登录入口官网
菠菜电子游戏登录入口
菠菜资讯导航大全
菠菜棋牌登录入口
菠菜棋牌改名叫什么了
菠菜新地址
最新菠菜棋牌大全
领悟棋牌v6.7.3版
大放水棋牌平台
齐齐乐棋牌
菠菜棋牌正版
菠菜娱乐官网入口
菠菜体育官网登录入口
菠菜体育app入口
完美体育app网页版入口
菠菜真人APP官网入口
菠萝蜜在线观看免费观看电视
菠菜真人集团创始人背景
菠菜真人国际的最新版本更新内容分享
菠菜真人app官方下载
菠菜真人视频在线观看免费
菠菜真人竞猜app下载
菠菜真人娱乐是正规平台吗安全吗
线上真人菠菜投注软件
菠菜真人免费视频观看动漫
'''
keywords = [keyword.strip() for keyword in KEYWORDS.split('\n') if keyword.strip()]
domains = """
http://www.hmc.edu.cn/
http://www.zjhu.edu.cn/
http://www.wzut.edu.cn/
http://www.zjcm.edu.cn/
http://www.zjpu.edu.cn/
http://www.jhc.edu.cn/
"""
domain_list = [domain.strip() for domain in domains.split('\n') if domain.strip()]

# 配置日志
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# 提取元数据
def extract_metadata(soup):
    title = soup.find('title').text.strip() if soup.find('title') else ''
    keywords_meta = soup.find('meta', attrs={'name': 'keywords'})
    keywords_content = keywords_meta['content'].strip() if keywords_meta else ''
    description_meta = soup.find('meta', attrs={'name': 'description'})
    description_content = description_meta['content'].strip() if description_meta else ''
    return {'title': title, 'keywords': keywords_content, 'description': description_content}

# 提取“大家还在搜”数据
def extract_related_searches(soup):
    related_div = soup.find('div', class_='c-title', string='大家还在搜')
    if related_div and related_div.next_sibling:
        data_tool = related_div.next_sibling.find('div', class_='sc-feedback')
        if data_tool and 'data-tool' in data_tool.attrs:
            try:
                tool_data = json.loads(data_tool['data-tool'].replace("'", '"'))
                related_words = tool_data.get('feedback', {}).get('suggest', {}).get('ext', {}).get('relation_words', '')
                return related_words
            except json.JSONDecodeError:
                return ''
    return ''

# 检测内容类型
def detect_content_type(url, soup):
    if "article" in url.lower() or soup.find('article'): return "文章"
    elif "product" in url.lower() or soup.find('div', class_=re.compile('product')): return "产品页面"
    elif "forum" in url.lower() or soup.find('div', class_=re.compile('post|thread')): return "论坛"
    elif "video" in url.lower() or soup.find('video'): return "视频"
    else: return "未知"

# 搜索关键词
async def search_keyword(page, keyword, all_results):
    search_url = f"https://m.baidu.com/s?wd={keyword}"
    try:
        await page.goto(search_url, timeout=30000)
        await page.wait_for_selector("div.result", timeout=30000)
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        related_searches = extract_related_searches(soup)
        results = soup.select('div.result')[:2]
        for result in results:
            data_log = result.get('data-log', '{}')
            link = eval(data_log.replace("'", '"')).get('mu', '无链接') if data_log else '无链接'
            title_element = result.select_one('h3') or result.select_one('a[class*="title"]')
            title_text = title_element.text.strip() if title_element else ''
            metadata, content_type = ({'title': '', 'keywords': '', 'description': ''}, '未知') if link == '无链接' else await fetch_page_data(page, link)
            # 添加UCI计算
            uci = await calculate_page_uci(page, link)
            all_results.append({
                '搜索关键词': keyword,
                '大家还在搜': related_searches,
                '内容类型': content_type,
                '网址': link,
                '页面标题': metadata['title'],
                '关键词': metadata['keywords'],
                '描述': metadata['description'],
                'UCI': uci
            })
    except Exception as e:
        logging.error(f"关键词 {keyword} 搜索失败：{e}")
    print(f"完成关键词：{keyword}")

# 获取页面数据
async def fetch_page_data(page, link):
    try:
        await page.goto(link, timeout=10000)
        await page.wait_for_selector("body", timeout=10000)
        page_content = await page.content()
        page_soup = BeautifulSoup(page_content, 'html.parser')
        return extract_metadata(page_soup), detect_content_type(page.url, page_soup)
    except Exception as e:
        logging.error(f"链接 {link} 访问失败：{e}")
        return {'title': '', 'keywords': '', 'description': ''}, '未知'

# 获取域名数据
async def get_domain_data(page, domain):
    try:
        await page.goto(domain, timeout=10000)
        await page.wait_for_selector("body", timeout=10000)
        content = len(await page.content())
        links = len(await page.query_selector_all("a"))
        return content, links
    except Exception as e:
        logging.error(f"域名 {domain} 访问失败：{e}")
        return 0, 0

# 计算 UCI
def calculate_uci(cf, lf, df, uf):
    return min(100, round(0.4 * cf + 0.3 * lf + 0.2 * df + 0.1 * uf, 2))

# 计算页面UCI
async def calculate_page_uci(page, link):
    try:
        content, links = await get_domain_data(page, link)
        cf = min(100, content / 10000 * 100)
        lf = min(100, links / 50 * 100)
        df = 100 if tldextract.extract(link).registered_domain in ["baidu.com", "qq.com", "163.com", "sohu.com", "sina.com"] else 0
        uf = 50
        uci = calculate_uci(cf, lf, df, uf)
        return uci
    except Exception as e:
        logging.error(f"计算页面 {link} 的UCI失败：{e}")
        return 0

# 处理域名
async def process_domain(page, domain):
    content, links = await get_domain_data(page, domain)
    cf = min(100, content / 10000 * 100)
    lf = min(100, links / 50 * 100)
    df = 100 if tldextract.extract(domain).registered_domain in ["baidu.com", "qq.com", "163.com", "sohu.com", "sina.com"] else 0
    uf = 50
    uci = calculate_uci(cf, lf, df, uf)
    return {"域名": domain, "UCI": uci}

# 主函数
async def main():
    start_time = time.strftime("%Y-%m-%d %H:%M")
    print(f"运行开始时间 {start_time}")
    logging.info(f"运行开始时间 {start_time}")

    all_results = []
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    excel_file = f"{script_name}_{time.strftime('%Y-%m-%d_%H.%M')}.xlsx"

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        page = await browser.new_page()

        # 处理关键词
        for i, keyword in enumerate(keywords, 1):
            await search_keyword(page, keyword, all_results)
            if i % 5 == 0 or i == len(keywords):
                df = pd.DataFrame(all_results)
                # 清理非法字符
                df = df.replace(ILLEGAL_CHARACTERS_RE, "", regex=True)
                print(f"\n当前数据结果（前5条）：\n{df.tail(5).to_string()}")
                print(f"完成进度：{i}/{len(keywords)} 个关键词")
                if i == 5:
                    df.to_excel(excel_file, index=False, sheet_name='Sheet1', engine='openpyxl')
                else:
                    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                        df.to_excel(writer, index=False, sheet_name='Sheet1')
                print(f"结果已保存至 {excel_file} 的 Sheet1")

        # 处理域名
        domain_results = [await process_domain(page, domain) for domain in domain_list]
        df_domains = pd.DataFrame(domain_results)
        # 清理非法字符
        df_domains = df_domains.replace(ILLEGAL_CHARACTERS_RE, "", regex=True)
        with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            df_domains.to_excel(writer, index=False, sheet_name='Sheet2')
        print(f"域名数据已保存至 {excel_file} 的 Sheet2")

        await browser.close()

    # 格式化 Excel
    wb = load_workbook(excel_file)
    ws1, ws2 = wb['Sheet1'], wb['Sheet2']
    ws1.freeze_panes, ws2.freeze_panes = 'A2', 'A2'
    ws1.auto_filter.ref, ws2.auto_filter.ref = ws1.dimensions, ws2.dimensions
    desc_col_idx = list(df.columns).index('描述') + 1
    for row in ws1.iter_rows(min_row=2, max_row=ws1.max_row, min_col=desc_col_idx, max_col=desc_col_idx):
        for cell in row:
            cell.alignment = Alignment(horizontal='right')

    # 将 Sheet1 的“大家还在搜”列复制到 Sheet3，按 & 分列，合并成单列并去重
    ws3 = wb.create_sheet('Sheet3')
    related_col_idx = list(df.columns).index('大家还在搜') + 1  # 找到“大家还在搜”列的索引
    for row in ws1.iter_rows(min_row=2, max_row=ws1.max_row, min_col=related_col_idx, max_col=related_col_idx):
        for cell in row:
            ws3[f'A{cell.row - 1}'].value = cell.value  # 复制到 Sheet3 的 A 列

    # 使用 split 分列并合并成单列
    all_values = []
    for row in ws3.iter_rows(min_row=1, max_row=ws3.max_row, min_col=1, max_col=1):
        for cell in row:
            if cell.value and '&' in str(cell.value):
                split_values = str(cell.value).split('&')
                all_values.extend(split_values)
            elif cell.value:
                all_values.append(str(cell.value))

    # 去重并写入 Sheet3 的 A 列
    unique_values = list(dict.fromkeys(all_values))  # 去重，保留顺序
    ws3.delete_rows(1, ws3.max_row)  # 清空 Sheet3
    for i, value in enumerate(unique_values, 1):
        ws3[f'A{i}'].value = value

    wb.save(excel_file)
    print(f"Excel 文件已格式化，且 Sheet1 的‘大家还在搜’列已复制到 Sheet3，按 & 分列，合并并去重")

    end_time = time.strftime("%Y-%m-%d %H:%M")
    print(f"运行结束时间 {end_time}")
    logging.info(f"运行结束时间 {end_time}")

if __name__ == "__main__":
    asyncio.run(main())
