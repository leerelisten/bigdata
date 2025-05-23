SET mapred.job.name="dws_place_order_firstsale#${datebuf}";
USE dw;
CREATE EXTERNAL TABLE IF NOT EXISTS dw.dws_place_order_firstsale
(
    ex_unionid STRING COMMENT 'ex_unionid',
    member_id  STRING COMMENT 'member_id',
    special_id STRING COMMENT 'special_id',
    sales_id   STRING COMMENT 'sales_id'
)
    COMMENT 'dws_place_order_firstsale'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/olap/db/dws_place_order_firstsale';

DROP TABLE aaa;
CREATE TEMPORARY TABLE aaa AS
SELECT ex_unionid
FROM ods.ods_place_contact
WHERE ex_unionid IS NOT NULL
  AND ex_unionid <> ''
  AND add_way = 202
  AND created_at >= '2024-11-28 00:00:00'
GROUP BY ex_unionid;
--
-- INSERT OVERWRITE TABLE dw.dws_place_order_firstsale
-- SELECT ex_unionid, member_id, special_id, sales_id
-- FROM (SELECT aa.ex_unionid
--            , aa.member_id
--            , c.special_id
--            , c.sales_id
--            , ROW_NUMBER() OVER (PARTITION BY c.member_id,c.special_id ORDER BY dt ASC) AS rn
--       FROM (SELECT a.ex_unionid, b.id AS member_id
--             FROM aaa a
--                      LEFT JOIN
--                      (SELECT * FROM ods.ods_place_member) b
--                      ON a.ex_unionid = b.unionid
--             GROUP BY a.ex_unionid, b.id) aa
--                LEFT JOIN (SELECT *
--                           FROM ods.ods_place_order_dt
--                           WHERE dt >= '2024-11-28'
--                             AND TO_DATE(created_at) >= '2024-11-28'
--                             AND member_status = 1
--                             AND trade_state IN ('SUCCESS', 'PREPARE')
--                             AND sales_id > 0) c
--                          ON aa.member_id = c.member_id) a
-- WHERE rn = 1;


INSERT OVERWRITE TABLE dw.dws_place_order_firstsale
SELECT aa.ex_unionid
     , aa.member_id
     , b.special_id
     , b.sales_id
FROM (SELECT a.ex_unionid, b.id AS member_id
      FROM aaa a
               LEFT JOIN
               (SELECT * FROM ods.ods_place_member) b
               ON a.ex_unionid = b.unionid
      GROUP BY a.ex_unionid, b.id) aa
         LEFT JOIN
     (
         SELECT id
              , member_id
              , special_id
              , sales_id
         FROM (
                  SELECT updated_time
                       , id
                       , member_id
                       , special_id
                       , sales_id
                       , ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_time ASC ) AS rn --找出最早的那条数据
                  FROM (
                           SELECT updated_time
                                , id
                                , member_id
                                , special_id
                                , sales_id
                           FROM (
                                    SELECT *,
                                           COUNT(DISTINCT sales_id) OVER (PARTITION BY member_id, special_id ) AS cnt
                                    FROM dw.dws_place_order_change_sale_records --place_order 变更记录
                                    WHERE table_type = 'UPDATE'
                                ) t
                           WHERE cnt >= 2 --找出换号的数据 2次及2次以上
                       ) p
              ) t1
         WHERE rn = 1
     ) b
     ON aa.member_id = b.member_id
WHERE b.member_id IS NOT NULL