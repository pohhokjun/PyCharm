def get_retention_report_sql(db, table):
    return f"""
    WITH date_range AS (
        SELECT DATE_SUB(DATE(CURDATE()), INTERVAL (n + 1) DAY) AS statics_date
        FROM (SELECT ROW_NUMBER() OVER () - 1 AS n 
              FROM information_schema.columns LIMIT 31) t
    ),
    base_data AS (
        SELECT site_id,
               DATE(first_deposit_time) AS cohort_date,
               member_id,
               bets,
               DATE(statics_date) AS activity_date
        FROM {bigdata}.{member_daily_statics}
        WHERE first_deposit_time BETWEEN DATE_SUB(CURDATE(), INTERVAL 61 DAY) AND CURDATE()
          AND statics_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 61 DAY) AND CURDATE()
    ),
    new_users AS (
        SELECT site_id, cohort_date, COUNT(DISTINCT member_id) AS 首存人数
        FROM base_data
        WHERE bets > 0
        GROUP BY site_id, cohort_date
    ),
    retention AS (
        SELECT site_id,
               activity_date AS statics_date,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 2 DAY)
                    AND bets > 0 THEN member_id END) AS `3日留存人数`,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 6 DAY)
                    AND bets > 0 THEN member_id END) AS `7日留存人数`,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 14 DAY)
                    AND bets > 0 THEN member_id END) AS `15日留存人数`,
               COUNT(DISTINCT CASE WHEN cohort_date = DATE_SUB(activity_date, INTERVAL 29 DAY)
                    AND bets > 0 THEN member_id END) AS `30日留存人数`
        FROM base_data
        GROUP BY site_id, activity_date
    )
    SELECT 
        COALESCE(n.site_id, r.site_id) AS 站点,
        DATE_FORMAT(d.statics_date, '%%Y-%%m-%%d') AS 日期,
        COALESCE(n.首存人数, 0) AS 首存人数,
        COALESCE(r.`3日留存人数`, 0) AS `3日留存人数`,
        COALESCE(r.`7日留存人数`, 0) AS `7日留存人数`,
        COALESCE(r.`15日留存人数`, 0) AS `15日留存人数`,
        COALESCE(r.`30日留存人数`, 0) AS `30日留存人数`
    FROM date_range d
    LEFT JOIN new_users n ON d.statics_date = n.cohort_date
    LEFT JOIN retention r ON d.statics_date = r.statics_date 
        AND (n.site_id = r.site_id OR (n.site_id IS NULL AND r.site_id IS NULL))
    ORDER BY 站点, d.statics_date ASC
    """


