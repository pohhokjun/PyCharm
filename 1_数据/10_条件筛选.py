def _11_login_members(self) -> pd.DataFrame:
    """查询登入注会员"""
    query = f"""
   SELECT DISTINCT u1_mi.id AS '会员ID'
   FROM {self.u1_1000}.member_info u1_mi
   WHERE u1_mi.last_login_time > '{self.start_date}'
   """
    if self.site_id is not None:
        query += f" AND u1_mi.site_id = {self.site_id}"
    return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)


def _14_recent_members_v3(self) -> pd.DataFrame:
    """查询VIP等级>=3且在开始日期前一个月从1号开始登录的会员"""
    start_date_obj = datetime.strptime(self.start_date, '%Y-%m-%d')
    one_month_before = start_date_obj - relativedelta(months=1, day=1)
    day_before_start_date = start_date_obj - relativedelta(days=1)
    query = f"""
   SELECT DISTINCT u1_mi.id AS '会员ID'
   FROM {self.u1_1000}.member_info u1_mi
   WHERE u1_mi.vip_grade >= 3
   AND u1_mi.last_login_time BETWEEN '{one_month_before.strftime('%Y-%m-%d')}' AND '{day_before_start_date.strftime('%Y-%m-%d')}'
   """
    if self.site_id is not None:
        query += f" AND u1_mi.site_id = {self.site_id}"
    return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)


def v0_no_bet_30_days(self) -> pd.DataFrame:
    """0 30天无投注名单"""
    query = f"""
    SELECT 
        mds.member_id AS '会员ID'
    FROM {self.bigdata}.member_daily_statics mds
    JOIN {self.u1_1000}.member_info mi ON mds.member_id = mi.id
    WHERE mi.vip_grade = '0'
    GROUP BY mds.member_id
    HAVING MAX(CASE WHEN mds.bets > 0 THEN mds.statics_date ELSE NULL END) < DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    """
    return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)


def v1_v2_no_bet_7_days(self) -> pd.DataFrame:
    """1-2 7天无投注名单"""
    query = f"""
    SELECT 
        mds.member_id AS '会员ID'
    FROM {self.bigdata}.member_daily_statics mds
    JOIN {self.u1_1000}.member_info mi ON mds.member_id = mi.id
    WHERE mi.vip_grade IN ('1', '2')
    GROUP BY mds.member_id
    HAVING MAX(CASE WHEN mds.bets > 0 THEN mds.statics_date ELSE NULL END) < DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    """
    return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)


def v4_above_no_bet_3_days(self) -> pd.DataFrame:
    """4及以上 3天无投注名单"""
    query = f"""
    SELECT 
        mds.member_id AS '会员ID'
    FROM {self.bigdata}.member_daily_statics mds
    JOIN {self.u1_1000}.member_info mi ON mds.member_id = mi.id
    WHERE mi.vip_grade IN ('4', '5', '6', '7', '8', '9', '10')
    GROUP BY mds.member_id
    HAVING MAX(CASE WHEN mds.bets > 0 THEN mds.statics_date ELSE NULL END) < DATE_SUB(CURDATE(), INTERVAL 3 DAY)
    """
    return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)
