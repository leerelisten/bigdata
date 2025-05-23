CREATE EXTERNAL TABLE IF NOT EXISTS dw.dws_xiaoe_periods_goods_day
(
    `xe_id`              string COMMENT '小鹅通id',
    `goods_name`         string COMMENT '期次/专栏名称',
    `goods_name_period`  string COMMENT '期次，格式yyMMdd',
    `img_url_compressed` string COMMENT '图片',
    `sync_time`          int COMMENT '同步时间',
    `sale_status`        tinyint COMMENT '上架状态',
    `is_deleted`         tinyint COMMENT '是否删除',
    `created_at`         string COMMENT '创建时间',
    `resource_type`      int COMMENT '商品类型',
    `xe_app_id`          string COMMENT '小鹅店铺APPID',
    `price_high`         int COMMENT '商品高价（取自最低价所在SKU对应划线价） 单位分',
    `sale_at`            string COMMENT '上架的时间',
    `label_name`         string COMMENT '标签D0 D1 L1',
    `cat`                string COMMENT '品类',
    `unit_pay_num`       decimal(12, 2) COMMENT '正价课订单数单位',
    `unit_pay_sum`       INT COMMENT '正价课GMV单位'
)
    COMMENT '小鹅通专栏期次表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/dws_xiaoe_periods_goods_day/';
-- 【250623期】【武当三丰秘传】舒筋养血·道门八段锦

INSERT
    OVERWRITE
    TABLE dw.dws_xiaoe_periods_goods_day
SELECT t1.xe_id
     , t1.goods_name
     , SUBSTR(t1.goods_name, 2, 6)                                                                  AS goods_name_period
     , t1.img_url_compressed
     , t1.sync_time
     , t1.sale_status
     , t1.is_deleted
     , t1.created_at
     , t1.resource_type
     , t1.xe_app_id
     , t1.price_high
     , t1.sale_at
     , t1.label_name
     , CASE
           WHEN TRIM(goods_name) LIKE '%筑基%' THEN '太极'
           WHEN TRIM(goods_name) LIKE '%柔骨活血武当八段锦%' THEN '太极'
           WHEN TRIM(goods_name) LIKE '%焕活养生%' THEN '养生'
           WHEN TRIM(goods_name) LIKE '%瑜伽%' THEN '瑜伽'
           WHEN TRIM(goods_name) LIKE '%变美%' THEN '中医变美'
           WHEN TRIM(goods_name) RLIKE '(系统课|试学课)$|形韵课|气韵|古典舞|舞蹈' THEN '古典舞' END AS cat
     , NVL(t3.order_coefficient, 0)                                                                 AS unit_pay_num
     , NVL(t3.order_amount, 0)                                                                      AS unit_pay_sum

FROM (SELECT *
      FROM ods.ods_xiaoe_special
      WHERE goods_name NOT RLIKE '测试|定金|占座|老学员|回放|会员专享|内部'
        AND goods_name RLIKE '柔骨活血|筑基|焕活养生|舒筋养血·道门八段锦|瑜伽|变美|形韵课|气韵|古典舞|舞蹈|三维驻颜|(系统课|试学课)$'
        AND is_deleted = 0
        -- 2025.5.23 跟贾雷霆沟通，使用螳螂课堂ID匹配螳螂创建的课程
        AND (price_high > 19900 or xe_id rlike '81136')
      UNION
      -- 添加道门八段锦课程正价课
      SELECT a.*
      FROM ods.ods_xiaoe_special a
               LEFT JOIN
           ods.ods_xiaoe_class b
           ON a.xe_id = b.resource_id
      WHERE b.type IN (112, 113)
      union all
--添加女儿情 中国舞
      SELECT a.*
      FROM ods.ods_xiaoe_special a
               LEFT JOIN
           ods.ods_xiaoe_class b
           ON a.xe_id = b.resource_id
      WHERE b.type IN (110, 6)
        AND a.goods_name RLIKE '中国舞'
        AND a.is_deleted = 0
        AND a.price_high > 19900
    ) t1
         LEFT JOIN
     (SELECT id
           , xe_app_id
           , creator_id
           , title
           , resource_id
           , created_at
           , updated_at
           , type
      FROM ods.ods_xiaoe_class --班级表
     ) t2
     ON t1.xe_id = t2.resource_id
         LEFT JOIN
     (
--              商品类型表
         SELECT id
              , order_coefficient --订单系数
              , order_amount      --订单金额
         FROM ods.ods_crm_product_types) t3
     ON t2.type = t3.id
;