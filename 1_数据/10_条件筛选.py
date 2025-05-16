def mongo_betting_venue(self, use_date_column: bool = False) -> pd.DataFrame:
    """查询 MongoDB 投注统计数据，按 venue 分组"""
    collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
    if not collections:
        return pd.DataFrame(
            columns=['日期', '会员ID', '场馆', '投注次数', '有效投注', '会员输赢'] if use_date_column else [
                '会员ID', '场馆', '投注次数', '有效投注', '会员输赢'])

    # 构建聚合管道
    pipeline = [
        {"$match": {
            "flag": 1,
            "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
        }},
        {"$sort": {"settle_time": 1}},
        {"$group": {
            "_id": {
                "member_id": "$member_id",
                "date": {"$dateToString": {"format": "%Y-%m-%d",
                                           "date": {"$toDate": "$settle_time"}}} if use_date_column else None,
                "venue": "$venue_name"  # 假设 MongoDB 文档有 venue_name 字段
            },
            "betting_count": {"$sum": 1},
            "valid_bet": {"$sum": "$valid_bet_amount"},
            "net_amount": {"$sum": "$net_amount"}
        }},
        {"$project": {
            "_id": 0,
            "日期": "$_id.date" if use_date_column else None,
            "会员ID": "$_id.member_id",
            "场馆": "$_id.venue",
            "betting_count": 1,
            "valid_bet": 1,
            "net_amount": 1
        }}
    ]
    if self.site_id is not None:
        pipeline[0]["$match"]["site_id"] = self.site_id
    # 移除 None 值
    pipeline = [{k: v for k, v in stage.items() if v is not None} for stage in pipeline]

    # 处理collections并提取 venue
    dfs = []
    for col in collections:
        venue = col[len(self.mongo_collection_prefix):]  # 提取 pull_order_game_ 后的字段
        pipeline_copy = pipeline.copy()
        df = self._process_mongo_collections([col], pipeline_copy)
        if not df.empty:
            df['场馆'] = venue  # 添加 venue 列
            dfs.append(df)

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(
        columns=['日期', '会员ID', '场馆', '投注次数', '有效投注', '会员输赢'] if use_date_column else ['会员ID','场馆','投注次数','有效投注','会员输赢'])

    if df.empty:
        return df

    # 类型优化
    df = df.astype({'会员ID': 'category', '场馆': 'string', 'betting_count': 'int32',
                    'valid_bet': 'float32', 'net_amount': 'float32'})
    if use_date_column:
        df['日期'] = df['日期'].astype('string')

    # 聚合会员统计
    group_cols = ['日期', '会员ID', '场馆'] if use_date_column else ['会员ID', '场馆']
    result = df.groupby(group_cols, observed=True).agg({
        'betting_count': 'sum',
        'valid_bet': 'sum',
        'net_amount': 'sum'
    }).reset_index().rename(columns={
        'betting_count': '投注次数',
        'valid_bet': '有效投注',
        'net_amount': '会员输赢'
    })

    # 定义场馆名称映射
    venue_mapping = {
        "CQ9ZR": "MT真人", "YXZR": "亚星真人", "EVOZR": "EVO真人", "SGCP": "双赢彩票",
        "CQ9BY": "MT弹珠", "CQ9DZ": "CQ9电子", "CRTY": "皇冠体育", "AGBY": "AG捕鱼",
        "PTDZ": "PT电子", "LHDJ": "雷火电竞", "PPZR": "PP真人", "IMTY": "IM体育",
        "DBBY": "DB捕鱼", "TCGCP": "TCG彩票", "IMDJ": "IM电竞", "BBINZR": "BBIN真人",
        "BYBY": "博雅捕鱼", "BYQP": "博雅棋牌", "BGZR": "BG真人", "EGDZ": "EG电子",
        "SBTY": "沙巴体育", "WMZR": "完美真人", "DBGGL": "DB刮刮乐", "GFQP": "GFG棋牌",
        "FBTY": "FB体育", "DBDJ": "多宝电竞", "DBQP": "多宝棋牌", "DBHX": "多宝哈希",
        "IMQP": "IM棋牌", "XMTY": "熊猫体育", "AGDZ": "AG电子", "DBDZ": "多宝电子",
        "AGZR": "AG真人", "DBZR": "多宝真人", "DBCP": "多宝彩票", "GFDZ": "GFG电子",
        "PPDZ": "PP电子", "PGDZ": "PG电子"
    }

    # 应用场馆名称映射
    result['场馆'] = result['场馆'].map(venue_mapping).fillna(result['场馆'])

    return result[result['有效投注'] > 0]


def mongo_betting_venue_column(self, use_date_column: bool = False) -> pd.DataFrame:
    """查询 MongoDB 投注统计数据，按 venue 分组并透视"""
    collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
    if not collections:
        # 返回一个空的 DataFrame，包含可能的索引列
        return pd.DataFrame(columns=['日期', '会员ID'] if use_date_column else ['会员ID'])

    # 构建聚合管道
    pipeline = [
        {"$match": {
            "flag": 1,
            "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
        }},
        {"$sort": {"settle_time": 1}},
        {"$group": {
            "_id": {
                "member_id": "$member_id",
                "date": {"$dateToString": {"format": "%Y-%m-%d",
                                           "date": {"$toDate": "$settle_time"}}} if use_date_column else None,
                "venue": "$venue_name"  # 假设 MongoDB 文档有 venue_name 字段
            },
            # 保留有效投注和会员输赢
            "valid_bet": {"$sum": "$valid_bet_amount"},
            "net_amount": {"$sum": "$net_amount"}
        }},
        {"$project": {
            "_id": 0,
            "日期": "$_id.date" if use_date_column else None,
            "会员ID": "$_id.member_id",
            "场馆": "$_id.venue",
            "valid_bet": 1,
            "net_amount": 1
        }}
    ]
    if self.site_id is not None:
        pipeline[0]["$match"]["site_id"] = self.site_id
    # 移除 None 值
    pipeline = [{k: v for k, v in stage.items() if v is not None} for stage in pipeline]

    # 执行聚合查询
    raw_df = self._process_mongo_collections(collections, pipeline)

    if raw_df.empty:
        return pd.DataFrame(columns=['日期', '会员ID'] if use_date_column else ['会员ID'])

    # 类型优化
    raw_df = raw_df.astype({'会员ID': 'category', '场馆': 'string',
                            'valid_bet': 'float32', 'net_amount': 'float32'})
    if use_date_column:
        raw_df['日期'] = raw_df['日期'].astype('string')

    # 定义场馆名称映射
    venue_mapping = {
        "CQ9ZR": "MT真人", "YXZR": "亚星真人", "EVOZR": "EVO真人", "SGCP": "双赢彩票",
        "CQ9BY": "MT弹珠", "CQ9DZ": "CQ9电子", "CRTY": "皇冠体育", "AGBY": "AG捕鱼",
        "PTDZ": "PT电子", "LHDJ": "雷火电竞", "PPZR": "PP真人", "IMTY": "IM体育",
        "DBBY": "DB捕鱼", "TCGCP": "TCG彩票", "IMDJ": "IM电竞", "BBINZR": "BBIN真人",
        "BYBY": "博雅捕鱼", "BYQP": "博雅棋牌", "BGZR": "BG真人", "EGDZ": "EG电子",
        "SBTY": "沙巴体育", "WMZR": "完美真人", "DBGGL": "DB刮刮乐", "GFQP": "GFG棋牌",
        "FBTY": "FB体育", "DBDJ": "多宝电竞", "DBQP": "多宝棋牌", "DBHX": "多宝哈希",
        "IMQP": "IM棋牌", "XMTY": "熊猫体育", "AGDZ": "AG电子", "DBDZ": "多宝电子",
        "AGZR": "AG真人", "DBZR": "多宝真人", "DBCP": "多宝彩票", "GFDZ": "GFG电子",
        "PPDZ": "PP电子", "PGDZ": "PG电子"
    }

    # 应用场馆名称映射
    raw_df.loc[:, '场馆'] = raw_df['场馆'].map(venue_mapping).fillna(raw_df['场馆'])

    # 定义透视表的索引列
    pivot_index = ['日期', '会员ID'] if use_date_column else ['会员ID']

    # 透视 DataFrame
    pivot_df = raw_df.pivot_table(
        index=pivot_index,
        columns='场馆',
        values=['valid_bet', 'net_amount'],  # 使用原始字段名进行透视
        aggfunc='sum',
        fill_value=0
    )

    metric_mapping = {
        'valid_bet': '有效投注',
        'net_amount': '会员输赢'
    }
    # 创建新的列名，格式为 "场馆名称指标名称"
    new_columns = [f'{col[1]}{metric_mapping[col[0]]}' for col in pivot_df.columns]
    pivot_df.columns = new_columns

    # 重置索引
    result = pivot_df.reset_index()

    return result


def mongo_betting_venue_ranking(self) -> pd.DataFrame:
    """查询 MongoDB 投注统计数据，按 venue 分组并计算场馆排名"""
    collections = [col for col in self.db.list_collection_names() if col.startswith(self.mongo_collection_prefix)]
    if not collections:
        # 返回一个空的 DataFrame，包含所有期望的列
        return pd.DataFrame(columns=['会员ID', '投注前1场馆', '投注前2场馆', '投注前3场馆',
                                     '公司输赢前1场馆', '公司输赢前2场馆', '公司输赢前3场馆',
                                     '公司输赢后1场馆', '公司输赢后2场馆', '公司输赢后3场馆'])

    # 构建聚合管道：按会员ID和场馆分组，计算有效投注总额和会员输赢总额
    pipeline = [
        {"$match": {
            "flag": 1,
            "settle_time": {"$gte": self.start_time, "$lte": self.end_time}
        }},
        {"$sort": {"settle_time": 1}}, # 排序通常不是必须的，但保留原结构
        {"$group": {
            "_id": {
                "member_id": "$member_id",
                "venue": "$venue_name"  # 假设 MongoDB 文档有 venue_name 字段
            },
            "total_valid_bet": {"$sum": "$valid_bet_amount"},
            "total_net_amount": {"$sum": "$net_amount"} # 会员输赢
        }},
        {"$project": {
            "_id": 0,
            "会员ID": "$_id.member_id",
            "场馆": "$_id.venue",
            "total_valid_bet": 1,
            "total_net_amount": 1 # 会员输赢
        }}
    ]
    if self.site_id is not None:
        pipeline[0]["$match"]["site_id"] = self.site_id

    raw_df = self._process_mongo_collections(collections, pipeline)

    if raw_df.empty:
         return pd.DataFrame(columns=['会员ID', '投注前1场馆', '投注前2场馆', '投注前3场馆',
                                     '公司输赢前1场馆', '公司输赢前2场馆', '公司输赢前3场馆',
                                     '公司输赢后1场馆', '公司输赢后2场馆', '公司输赢后3场馆'])

    # 类型优化
    raw_df = raw_df.astype({'会员ID': 'category', '场馆': 'string',
                            'total_valid_bet': 'float32', 'total_net_amount': 'float32'})

    # 定义场馆名称映射 (保留原有的映射)
    venue_mapping = {
        "CQ9ZR": "MT真人", "YXZR": "亚星真人", "EVOZR": "EVO真人", "SGCP": "双赢彩票",
        "CQ9BY": "MT弹珠", "CQ9DZ": "CQ9电子", "CRTY": "皇冠体育", "AGBY": "AG捕鱼",
        "PTDZ": "PT电子", "LHDJ": "雷火电竞", "PPZR": "PP真人", "IMTY": "IM体育",
        "DBBY": "DB捕鱼", "TCGCP": "TCG彩票", "IMDJ": "IM电竞", "BBINZR": "BBIN真人",
        "BYBY": "博雅捕鱼", "BYQP": "博雅棋牌", "BGZR": "BG真人", "EGDZ": "EG电子",
        "SBTY": "沙巴体育", "WMZR": "完美真人", "DBGGL": "DB刮刮乐", "GFQP": "GFG棋牌",
        "FBTY": "FB体育", "DBDJ": "多宝电竞", "DBQP": "多宝棋牌", "DBHX": "多宝哈希",
        "IMQP": "IM棋牌", "XMTY": "熊猫体育", "AGDZ": "AG电子", "DBDZ": "多宝电子",
        "AGZR": "AG真人", "DBZR": "多宝真人", "DBCP": "多宝彩票", "GFDZ": "GFG电子",
        "PPDZ": "PP电子", "PGDZ": "PG电子"
    }
    # 应用场馆名称映射
    raw_df.loc[:, '场馆'] = raw_df['场馆'].map(venue_mapping).fillna(raw_df['场馆'])

    # 计算公司输赢 (公司输赢 = -会员输赢)
    raw_df['total_company_win'] = -raw_df['total_net_amount']

    # 为每个会员计算场馆排名
    def get_ranked_venues(group):
        # 投注排名 (有效投注从高到低)
        bet_ranked = group.sort_values(by='total_valid_bet', ascending=False).head(3)['场馆'].tolist()
        # 公司输赢排名 (公司输赢从高到低，即会员输赢从低到高)
        company_win_ranked_top = group.sort_values(by='total_company_win', ascending=False).head(3)['场馆'].tolist()
         # 公司输赢排名 (公司输赢从低到高，即会员输赢从高到低)
        company_win_ranked_bottom = group.sort_values(by='total_company_win', ascending=True).head(3)['场馆'].tolist()


        # 填充不足3个场馆的情况
        bet_ranked += [''] * (3 - len(bet_ranked))
        company_win_ranked_top += [''] * (3 - len(company_win_ranked_top))
        company_win_ranked_bottom += [''] * (3 - len(company_win_ranked_bottom))

        return pd.Series({
            '投注前1场馆': bet_ranked[0],
            '投注前2场馆': bet_ranked[1],
            '投注前3场馆': bet_ranked[2],
            '公司输赢前1场馆': company_win_ranked_top[0],
            '公司输赢前2场馆': company_win_ranked_top[1],
            '公司输赢前3场馆': company_win_ranked_top[2],
            '公司输赢后1场馆': company_win_ranked_bottom[0],
            '公司输赢后2场馆': company_win_ranked_bottom[1],
            '公司输赢后3场馆': company_win_ranked_bottom[2],
        })

    # 按会员ID分组并应用排名函数
    ranked_venues_df = raw_df.groupby('会员ID', observed=True).apply(get_ranked_venues).reset_index()

    return ranked_venues_df


def _11_member_dividend(self) -> pd.DataFrame:
    """查询会员历史累计统计信息，筛选指定 site_id"""
    query = f"""
    SELECT
        a1_md.site_id AS 站点ID,
        a1_md.activity_title AS 活动标题,
        a1_oci.title AS 标题,
        a1_md.member_id AS 会员ID,
        a1_md.member_name AS 会员账号,
        a1_md.member_grade AS 会员等级,
        a1_md.bill_no AS 订单号,
        CASE
            WHEN a1_md.wallet_category = 1 THEN '中心钱包'
            WHEN a1_md.wallet_category = 2 THEN '场馆钱包'
            ELSE a1_md.wallet_category
        END AS 钱包类别,
        a1_md.flow_times AS 流水倍数,
        a1_md.money AS 红利金额,
        CASE
            WHEN a1_md.status = 1 THEN '处理中'
            WHEN a1_md.status = 2 THEN '成功'
            WHEN a1_md.status = 3 THEN '失败'
            ELSE a1_md.status
        END AS 状态,
        CASE
            WHEN a1_md.issue_type = 1 THEN '手动发放'
            WHEN a1_md.issue_type = 2 THEN '自动发放'
            ELSE a1_md.issue_type
        END AS 发行类型,
        COALESCE(sv.dict_value, a1_md.activity_type) AS '活动类型',
        a1_md.created_at AS 申请时间,
        a1_md.applicant_remark AS 申请备注,
        a1_md.updated_at AS 发放时间,
        a1_md.check_user AS 审核用户,
        a1_md.check_remark AS 审核备注,
        a1_md.applicant AS 操作人
    FROM activity_1000.member_dividend a1_md
    LEFT JOIN activity_1000.operation_activity_info a1_oci
        ON a1_md.activity_id = a1_oci.id
    LEFT JOIN (
        SELECT code, dict_value
        FROM (
            SELECT
                code,
                dict_value,
                dict_code,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY code) AS rn
            FROM control_1000.sys_dict_value
            WHERE dict_code IN ('activity_type')
        ) t
        WHERE rn = 1
    ) sv ON a1_md.activity_type = sv.code
    WHERE a1_md.updated_at BETWEEN '{self.start_time}' AND '{self.end_time}'
    AND a1_md.category NOT IN (999555)
    """
    if self.site_id is not None:
        query += f" AND a1_md.site_id = {self.site_id}"
    return pd.concat(pd.read_sql(query, self.engine, chunksize=5000), ignore_index=True)


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

