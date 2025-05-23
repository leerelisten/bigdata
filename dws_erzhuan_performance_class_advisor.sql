
CREATE TABLE IF NOT EXISTS dws.dws_erzhuan_performance_class_advisor
(

    goods_name      STRING COMMENT '商品名称',
    sales_name      STRING COMMENT '班主任',
    student_all_cnt bigint COMMENT '学员数',
    change_cnt      bigint COMMENT '认领学员数',
    refund_cnt      bigint COMMENT '退费学员数',
    delay_cnt       bigint COMMENT '延期学员数',
    vaild_cnt       bigint COMMENT '有效学员数'
) COMMENT '教服二转业绩班主任信息'
    PARTITIONED BY (date_month STRING COMMENT '分区') -- 假设有日期分区
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;



CREATE TEMPORARY TABLE student_info AS
SELECT a.xiaoe_member_id
     , a.xiaoe_order_resource_id
     , d.goods_name
     , b.title
     , c.mobile
     , c.contact_ex_nickname
     , REGEXP_REPLACE(REGEXP_REPLACE(e.name, '[a-zA-Z0-9]', ''), '（新）', '') AS sales_name
     , CASE
           WHEN a.class_wx_relation_status = '1' THEN '未添加微信'
           WHEN a.class_wx_relation_status = '2' THEN '已添加微信'
           WHEN a.class_wx_relation_status = '3' THEN '用户单删'
           WHEN a.class_wx_relation_status = '4' THEN '内部单删'
           ELSE '未知' END                                                    AS class_wx_relation_raltion
     , order_state  --订单状态
     , delay_status --延期状态
     , IF(f.xiaoe_member_id IS NOT NULL, 1, 0)                              AS if_change_id
FROM ods.ods_xiaoe_vip_member a
         JOIN ods.ods_xiaoe_class b
              ON a.class_id = b.id
         LEFT JOIN ods.ods_xiaoe_member c
                   ON a.xiaoe_member_id = c.id
         LEFT JOIN ods.ods_xiaoe_special d
                   ON a.xiaoe_order_resource_id = d.xe_id
         LEFT JOIN dw.tdlive_place_sales e
                   ON a.class_sales_id = e.id
         LEFT JOIN (
    --换号学员信息
    SELECT xiaoe_member_id
         , relate_member_id
    FROM ods.ods_crm_vip_change_account_record
    WHERE qw_sp_status = '2'
      AND relate_member_id > 0
      AND reason NOT LIKE '%测试%'
    GROUP BY xiaoe_member_id
           , relate_member_id
) f
                   ON a.xiaoe_member_id = f.xiaoe_member_id
WHERE b.title RLIKE '250306|250318' --每个月需调整
  AND b.title LIKE '%筑基%'
  AND b.title NOT RLIKE '国际版|精修|同修|私域'
;

--计算聚合数据

CREATE TEMPORARY TABLE erzhuan_result AS
SELECT title                                                                         AS goods_name
     , sales_name
     , COUNT(DISTINCT xiaoe_member_id)                                               AS student_all_cnt
     , COUNT(DISTINCT CASE WHEN if_change_id = 1 THEN xiaoe_member_id ELSE NULL END) AS change_cnt

     , COUNT(
        DISTINCT CASE WHEN order_state IN (10, 11) THEN xiaoe_member_id ELSE NULL END)        AS refund_cnt
     , COUNT(DISTINCT CASE WHEN delay_status IN (1) THEN xiaoe_member_id ELSE NULL END)       AS delay_cnt

     , COUNT(DISTINCT CASE
                          WHEN order_state NOT IN (10, 11) AND delay_status NOT IN (1) AND if_change_id NOT IN (1)
                              THEN xiaoe_member_id
                          ELSE NULL END)                                             AS vaild_cnt
FROM student_info
GROUP BY title
       , sales_name

;


--插入表
INSERT  OVERWRITE TABLE dws.dws_erzhuan_performance_class_advisor PARTITION (date_month = '2025-04')
SELECT
     goods_name
    ,sales_name
    ,student_all_cnt
    ,change_cnt
    ,refund_cnt
    ,delay_cnt
    ,vaild_cnt
from   erzhuan_result