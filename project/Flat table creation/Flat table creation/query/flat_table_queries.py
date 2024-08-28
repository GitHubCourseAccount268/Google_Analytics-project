records_check_query='''SELECT COUNT(*) as Records FROM `{}.{}.ga_sessions_*`
    WHERE _TABLE_SUFFIX = REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING), '-', '')'''