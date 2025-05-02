
import os
import asyncio
import datetime
import pandas as pd
from sqlalchemy import create_engine
from telegram import Bot
from telegram.error import TelegramError

# 配置
FOLDER_PATH = 'C:/Henvita/1_定时注单导出/收费站'
SITE_ID = '5000'
TELEGRAM_BOT_TOKEN = '7750313084:AAGci5ANeeyEacKJUESQuDHYyy8tLdl9m7Q'
CHAT_ID = '7523061850'

# 初始化 Telegram Bot
bot = Bot(token=TELEGRAM_BOT_TOKEN)

def export_data():
  """从数据库导出数据并生成 Excel 文件"""
  engine = create_engine('mysql+pymysql://bigdata:uvb5SOSmLH8sCoSU@18.178.159.230:3366/u1_1000', pool_pre_ping=True)

  # 第一个查询：详细数据
  query1 = """
  SELECT 
      m.id AS member_id, 
      m.site_id AS 站点ID, 
      m.name AS 会员账号, 
      m.sex AS 性别, 
      m.status AS 状态, 
      m.top_id AS 代理ID, 
      m.top_name AS 上级代理, 
      m.is_agent AS 是否代理, 
      m.birthday AS 生日, 
      m.area_code AS 地区代码, 
      m.nick_name AS 昵称, 
      m.last_login_time AS 最后登录时间, 
      m.source_url AS 来源链接, 
      m.register_device AS 注册设备, 
      m.login_device AS 登录设备, 
      m.vip_grade AS VIP等级, 
      m.phone_realname_check AS 手机实名验证, 
      m.svip AS SVIP, 
      m.created_at AS 注册时间,
      ANY_VALUE(w.available_money) AS 中心钱包,
      ANY_VALUE(w.agent_money) AS 佣金账户,
      ANY_VALUE(w.valet_money) AS 代存账户,
      ANY_VALUE(w.usdt_money) AS 虚拟币账户
  FROM 
      u1_1000.member_info m
  LEFT JOIN 
      finance_1000.member_wallet w
  ON 
      m.id = w.member_id
  WHERE 
      m.site_id = %s
  GROUP BY 
      m.id
  """

  # 第二个查询：汇总数据
  query2 = """
  SELECT  
      SUM(w.available_money) AS 中心钱包, 
      SUM(w.agent_money) AS 佣金账户, 
      SUM(w.valet_money) AS 代存账户, 
      SUM(w.usdt_money) AS 虚拟币账户
  FROM finance_1000.member_wallet w
  JOIN u1_1000.member_info m
  ON w.member_id = m.id
  WHERE m.site_id = %s
  GROUP BY m.site_id
  """

  # 执行查询
  df1 = pd.read_sql_query(query1, engine, params=(SITE_ID,))
  df2 = pd.read_sql_query(query2, engine, params=(SITE_ID,))

  # 添加“站点”列到汇总数据和详细数据
  df1.insert(0, '站点', '亚洲之星')
  df2.insert(0, '站点', '亚洲之星')

  # 创建 Excel 文件
  output_file = f"{FOLDER_PATH}/账户余额_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
  with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
      df2.to_excel(writer, sheet_name='汇总数据', index=False)
      df1.to_excel(writer, sheet_name='详细数据', index=False)

  # 格式化汇总数据为 Telegram 文字
  today = datetime.datetime.now().strftime('%Y-%m-%d')
  summary_text = (
      f"亚洲之星 {today} 汇总数据:\n"
      f"中心钱包: {df2['中心钱包'].iloc[0]:,.2f}\n"
      f"佣金账户: {df2['佣金账户'].iloc[0]:,.2f}\n"
      f"代存账户: {df2['代存账户'].iloc[0]:,.2f}\n"
      f"虚拟币账户: {df2['虚拟币账户'].iloc[0]:,.2f}"
  )

  return output_file, summary_text

async def send_file(bot, file_path, chat_id, summary_text):
  """通过 Telegram 发送文件和文字作为 caption"""
  try:
      with open(file_path, 'rb') as file:
          await bot.send_document(chat_id=chat_id, document=file, caption=summary_text)
      print(f"文件已发送: {os.path.basename(file_path)}")
  except TelegramError as e:
      print(f"发送文件 {os.path.basename(file_path)} 时出错: {e}")

def delete_files(directory):
  """删除目录中的所有文件"""
  try:
      for filename in os.listdir(directory):
          file_path = os.path.join(directory, filename)
          if os.path.isfile(file_path):
              os.remove(file_path)
              print(f"删除文件: {file_path}")
  except Exception as e:
      print(f"删除文件时出错: {e}")

def get_time_until(target_hour, target_minute):
  """计算到下一次目标时间的秒数"""
  now = datetime.datetime.now()
  next_target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
  if now >= next_target:
      next_target += datetime.timedelta(days=1)
  seconds_until = (next_target - now).total_seconds()
  print(f"下一次执行将在 {next_target.strftime('%Y-%m-%d %H:%M:%S')} 发生")
  return seconds_until

async def main():
  """主程序：定时执行数据导出、发送和删除"""
  # 立即执行一次
  try:
      output_file, summary_text = export_data()
      await send_file(bot, output_file, CHAT_ID, summary_text)
      delete_files(FOLDER_PATH)
  except Exception as e:
      print(f"首次执行出错: {e}")

  # 每小时执行一次
  while True:
      await asyncio.sleep(86400)
      try:
          output_file, summary_text = export_data()
          await send_file(bot, output_file, CHAT_ID, summary_text)
          delete_files(FOLDER_PATH)
      except Exception as e:
          print(f"主程序执行出错: {e}")

if __name__ == "__main__":
  asyncio.run(main())

