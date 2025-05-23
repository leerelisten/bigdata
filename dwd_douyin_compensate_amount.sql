CREATE TABLE IF NOT EXISTS dwd.dwd_douyin_compensate_amount
(
    advertiser_id     string COMMENT '账户id',
    compensate_amount string COMMENT '赔付金额',
    start_date        string COMMENT '开始日期',
    end_date          string COMMENT '结束日期'

) COMMENT '抖音赔付金额表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
;
-- 建表思路
-- 1.将 PAID的金额摊入到 IN_EFFECT对应的日期中，得到计划粒度 start_date\end_date
-- 2.多笔金额情况，求和成一笔金额，摊入到对应的IN_EFFECT日期中
-- 3..聚合到账户维度，关联账户维表，只取抖音直播账户
-- 注：一个计划iD只有一笔赔付金额，不会有多笔。可以不考虑计划iD有多段的情况

-- 后面合并逻辑：	现在需要将 API拉取计算的赔付数据 和 飞书表格数据 做合并
-- 取飞书表格数据end_date <= 2025-04-18  为分界点，然后 union all 拼上 API拉取计算的赔付数据

--处理计划表
DROP TABLE ad_id_compenstate_data;
CREATE TEMPORARY TABLE ad_id_compenstate_data AS
SELECT t1.advertiser_id
     , t1.promotion_id
     , t2.compensate_amount
     , t1.start_date
     , t1.end_date
FROM (
         -- 取 状态在 DEFAULT_STATUS: IN_EFFECT: 成本保障生效中    INVALID: 成本保障已失效 CONFIRMING: 成本保障确认中 ENDED: 成本保障已结束 中的数据
         SELECT advertiser_id
              , promotion_id
              , MIN(cdate)    start_date
              , MAX(cdate) AS end_date
         FROM ods.marketing_cost_protect_status_log
         WHERE compensate_amount = 0
           AND compensate_status IN ('IN_EFFECT')
         GROUP BY advertiser_id
                , promotion_id
     ) t1
         LEFT JOIN
     (
         --取paid 赔款到金额
         SELECT advertiser_id
              , promotion_id
              , compensate_amount
         FROM ods.marketing_cost_protect_status_log
         WHERE compensate_amount > 0
           AND compensate_status = 'PAID'
         GROUP BY advertiser_id
                , promotion_id
                , compensate_amount
     ) t2
     ON t1.advertiser_id = t2.advertiser_id
         AND t1.promotion_id = t2.promotion_id
;


--API拉取的数据
DROP TABLE cost_info_compensate_data;
CREATE TEMPORARY TABLE cost_info_compensate_data AS
SELECT advertiser_id
     , compensate_amount
     , start_date
     , end_date
FROM (
         --按照账户维度聚合   SUM(赔付金额)	MIN(开始日期)	MAX(结束日期)
         SELECT advertiser_id
              , start_date
              , end_date
              , SUM(compensate_amount) AS compensate_amount

         FROM ad_id_compenstate_data
         WHERE compensate_amount IS NOT NULL
         GROUP BY advertiser_id
                , start_date
                , end_date
     ) a
         JOIN (
    --只取抖音账户
    SELECT cost_id
    FROM (
             SELECT *
                  , ROW_NUMBER() OVER (PARTITION BY cost_id ORDER BY updated_at DESC) AS rnum
             FROM dim.dim_place_costname) tmp
    WHERE rnum = 1
      --抖音直播账户
      AND d_pos IN (
                    '头条直播付费流', '本地推直播', '头条直播免费流'
        )
) b
              ON a.advertiser_id = b.cost_id
;



INSERT OVERWRITE TABLE dwd.dwd_douyin_compensate_amount PARTITION (dt = '${datebuf}')
SELECT advertiser_id
     , compensate_amount
     , start_date
     , end_date
FROM cost_info_compensate_data
union all
SELECT
    account_id as  advertiser_id
     ,compensation_amount as compensate_amount
     ,start_date
     ,end_date

from (
         SELECT account_id
              , SUBSTR(CAST(FROM_UNIXTIME(CAST(SUBSTR(start_date, 1, 10) AS BIGINT), 'yyyy-MM-dd') AS STRING), 1,
                       10)                                                                                          AS start_date
              , SUBSTR(CAST(FROM_UNIXTIME(CAST(SUBSTR(end_date, 1, 10) AS BIGINT), 'yyyy-MM-dd') AS STRING), 1,
                       10)                                                                                          AS end_date
              , SUM(compensation_amount)                                                                            AS compensation_amount
         FROM ods.ods_compensation_records
         WHERE dt = '2025-05-13'  --这里写固定日期 以后不更新了
           AND account_id <> ''
         GROUP BY account_id
                , SUBSTR(CAST(FROM_UNIXTIME(CAST(SUBSTR(start_date, 1, 10) AS BIGINT), 'yyyy-MM-dd') AS STRING), 1, 10)
                , SUBSTR(CAST(FROM_UNIXTIME(CAST(SUBSTR(end_date, 1, 10) AS BIGINT), 'yyyy-MM-dd') AS STRING), 1, 10)
     )a
where end_date  <= '2025-04-18'