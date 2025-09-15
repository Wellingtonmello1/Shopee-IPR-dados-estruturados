WITH base AS (
    SELECT
        ip.item_id,
        ip.status,
        sc.approve_or_reject_date AT TIME ZONE 'America/Sao_Paulo' AS approve_date
    FROM
         mp_seller.dwd_brand_ip_violation_case_listing_df__br_live ip
    LEFT JOIN (
        SELECT
            case_id,
            MIN(from_unixtime(modify_timestamp) AT TIME ZONE 'America/Sao_Paulo') AS approve_or_reject_date
        FROM
            mp_seller.dwd_brand_ip_violation_case_listing_df__br_live
        WHERE status != 'pending'
        GROUP BY case_id
    ) sc ON ip.case_id = sc.case_id
    WHERE sc.approve_or_reject_date IS NOT NULL
),

ultimas_semanas AS (
    SELECT 
        item_id,
        status,
        EXTRACT(WEEK FROM approve_date) AS volume_week,
        EXTRACT(MONTH FROM approve_date) AS volume_month,
        EXTRACT(YEAR FROM approve_date) AS volume_year
    FROM base
    WHERE date(approve_date) BETWEEN date_add('day', -90, CURRENT_DATE) AND CURRENT_DATE
)

SELECT
    -- totais semanais
    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -1 THEN item_id END) AS W1,
    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -2 THEN item_id END) AS W2,
    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -3 THEN item_id END) AS W3,
    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -4 THEN item_id END) AS W4,

    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -1 AND status = 'rejected' THEN item_id END) AS rejeitados_1,
    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -2 AND status = 'rejected' THEN item_id END) AS rejeitados_2,
    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -3 AND status = 'rejected' THEN item_id END) AS rejeitados_3,
    COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -4 AND status = 'rejected' THEN item_id END) AS rejeitados_4,

    -- fail rate semanal
    ROUND(
        CASE WHEN COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -1 THEN item_id END) = 0 THEN 0
             ELSE COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -1 AND status = 'rejected' THEN item_id END) * 100.0 /
                  COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -1 THEN item_id END) END, 2
    ) AS fail_rate_w1,

    ROUND(
        CASE WHEN COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -2 THEN item_id END) = 0 THEN 0
             ELSE COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -2 AND status = 'rejected' THEN item_id END) * 100.0 /
                  COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -2 THEN item_id END) END, 2
    ) AS fail_rate_w2,

    ROUND(
        CASE WHEN COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -3 THEN item_id END) = 0 THEN 0
             ELSE COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -3 AND status = 'rejected' THEN item_id END) * 100.0 /
                  COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -3 THEN item_id END) END, 2
    ) AS fail_rate_w3,

    ROUND(
        CASE WHEN COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -4 THEN item_id END) = 0 THEN 0
             ELSE COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -4 AND status = 'rejected' THEN item_id END) * 100.0 /
                  COUNT(DISTINCT CASE WHEN volume_week = week_of_year(CURRENT_DATE) -4 THEN item_id END) END, 2
    ) AS fail_rate_w4,

    -- totais mensais últimos 3 meses
   COUNT(DISTINCT CASE WHEN volume_year = year(CURRENT_DATE) AND volume_month = month(CURRENT_DATE) -1 THEN item_id END) AS M_1,
    COUNT(DISTINCT CASE WHEN volume_year = year(CURRENT_DATE) AND volume_month = month(CURRENT_DATE) -2 THEN item_id END) AS M_2,
    COUNT(DISTINCT CASE WHEN volume_year = year(CURRENT_DATE) AND volume_month = month(CURRENT_DATE) -3 THEN item_id END) AS M_3

    FROM ultimas_semanas;