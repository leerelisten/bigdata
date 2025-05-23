SET mapred.job.name="c_app_sanzhuan_hrbp_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_sanzhuan_hrbp_dashboard
(
    d_date            STRING COMMENT '日期',    
    grouptype         STRING COMMENT '分组类型',
	title             STRING COMMENT '期次',
    sale_name         STRING COMMENT '班主任',
    user_num             INT COMMENT '例子数(个)',
    pay_num              INT COMMENT '结课订单数(单)',
    convert_rate       FLOAT COMMENT '转化率(%)',
    pay_num_out          INT COMMENT '转化期外成单数(单)',
    pay_sum            FLOAT COMMENT '结课GMV(元)',
    pay_sum_out        FLOAT COMMENT '结课外GMV(元)',
    pay_sum_all        FLOAT COMMENT '总GMV(元)'
)
    COMMENT '二转期次销售结果报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_sanzhuan_hrbp_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;
set hive.strict.checks.cartesian.product=false;
SET hive.mapred.mode=nonstrict;



--20250110  添加处理换号后数据
DROP TABLE IF EXISTS different_phase;
CREATE TEMPORARY TABLE different_phase AS
--把换号号对应的期次不同的数据拿出来
SELECT a.relate_member_id, -- 新会员ID
       a.from_xiaoe_user_id,
       a.special_id,
       b.xiaoe_order_resource_id
FROM (
         SELECT relate_member_id, -- 新会员ID
                member_id as xiaoe_member_id,  -- 老会员ID
                from_xiaoe_user_id,
                special_id
         FROM dwd.dwd_crm_vip_change_account_record -- 换号表
         WHERE dt = '${datebuf}'
           AND reason NOT LIKE '%测试%'
     ) a
         LEFT JOIN
     (
         SELECT class_sales_id  -- 班主任id
              , xiaoe_member_id -- 用户id
              , xiaoe_order_resource_id
              , class_id        -- 班级id
         FROM ods.ods_xiaoe_vip_member
     ) b
     ON a.relate_member_id = b.xiaoe_member_id
WHERE a.special_id <> b.xiaoe_order_resource_id --拿出换号后期次 换号后期次要与换号中对应的期次不同
;


--拼接上换号前后都相同的期次 找出换号后对应的期次
DROP TABLE IF EXISTS change_account_data;
CREATE TEMPORARY TABLE change_account_data AS
SELECT relate_member_id
     , from_xiaoe_user_id
     , xiaoe_order_resource_id AS special_id
FROM different_phase

UNION ALL
SELECT a.relate_member_id, -- 新会员ID
       a.from_xiaoe_user_id,
       a.special_id
FROM (
         SELECT relate_member_id, -- 新会员ID
                member_id as xiaoe_member_id,  -- 老会员ID
                from_xiaoe_user_id,
                special_id
         FROM dwd.dwd_crm_vip_change_account_record -- 换号表
         WHERE dt = '${datebuf}'
           AND reason NOT LIKE '%测试%'
     ) a
         LEFT JOIN different_phase b
                   ON a.relate_member_id = b.relate_member_id
WHERE b.relate_member_id IS NULL
;


-- 班级信息加日期打标
DROP TABLE xiaoe_class;
CREATE TEMPORARY TABLE xiaoe_class AS
select title
     , id
     , resource_id                                                 -- 期次id
     , substr(title, 2, 6)                     AS goods_name_period
     , date_format(date_add(from_unixtime(unix_timestamp(concat('20', substr(title, 2, 6)), 'yyyyMMdd')), 43),
                   'yyyy-MM-dd')               AS end_date-- 结课时间
     , date_format(date_add(from_unixtime(unix_timestamp(concat('20', substr(title, 2, 6)), 'yyyyMMdd')), 30),
                   'yyyy-MM-dd')               AS start_date-- 销转开始时间
     , substr(date_format(date_add(from_unixtime(unix_timestamp(concat('20', substr(title, 2, 6)), 'yyyyMMdd')), 43),
                          'yyyy-MM-dd'), 1, 7) AS attributed_month-- 归属月份
     , date_format(
        add_months(date_add(from_unixtime(unix_timestamp(concat('20', substr(title, 2, 6)), 'yyyyMMdd')), 43), 1),
        'yyyy-MM-01')                          AS caculate_end_date-- 追单到月末时间
from ods.ods_xiaoe_class
where type = 13 -- 42天炼气
  and title not like '%测试%';


-- 学员信息
DROP TABLE vip_member;
CREATE TEMPORARY TABLE vip_member AS
SELECT a.class_sales_id  -- 班主任id
     , a.xiaoe_member_id -- 用户id
     , a.xiaoe_order_resource_id
     , a.class_id        -- 班级id
FROM (
         SELECT class_sales_id  -- 班主任id
              , xiaoe_member_id -- 用户id
              , xiaoe_order_resource_id
              , class_id        -- 班级id
         FROM ods.ods_xiaoe_vip_member
         WHERE order_state = 1 -- 订单状态成功
           AND delay_status = 0
     ) a
         LEFT JOIN change_account_data b --关联换号数据 排除掉换号后对应的例子 保留换号前例子
                   ON a.xiaoe_member_id = b.relate_member_id
                       AND a.xiaoe_order_resource_id = b.special_id
WHERE b.relate_member_id IS NULL
  AND b.special_id IS NULL
;
-- 延期状态正常
--and xiaoe_order_resource_id = 'p_66b1e4b0e4b0d84dbbf5ec41';



-- 订单信息
DROP TABLE xiaoe_order;
CREATE TEMPORARY TABLE xiaoe_order AS
select a.*, b.id as member_id, c.goods_name
from ods.ods_xiaoe_order_dt a
         left join ods.ods_xiaoe_member b
                   on a.user_id = b.xe_id
         left join ods.ods_xiaoe_special c
                   on a.resource_id = c.xe_id
where a.dt = '${datebuf}'
  and a.resource_type in ('6', '100006', '8', '41', '100008')
  and a.xiaoe_order_type <> '开放API导入订单'
  and a.price > '0'
  and a.xe_app_id = 'appcafhwq5q8671';



--每一期次对应的全部订单数据
DROP TABLE order_all;
CREATE TEMPORARY TABLE order_all AS
select b.owner_id, b.price, b.goods_name, b.member_id, b.pay_time, case when c.class_sales_id is not null then 1 else 0 end  as teacher_if_current,
       case when a.resource_id = b.owner_class then 1 else 0 end as period_if_current,
       case when d.class_sales_id is not null then 1 else 0 end  as teacher_if_student,
       a.title,
       a.resource_id
from xiaoe_class a
         left join xiaoe_order b
                   on a.start_date <= b.pay_time and a.caculate_end_date > b.pay_time
         left join (select class_sales_id from vip_member group by class_sales_id) c
                   on b.owner_id = c.class_sales_id
         left join vip_member d
                   on b.member_id = d.xiaoe_member_id and a.id = d.class_id;
--where b.goods_name rlike '线上班|线上+线下|特殊申请通道'


--获取三转订单明细
DROP TABLE order_3z;
CREATE TEMPORARY TABLE order_3z AS
select *
from order_all
where goods_name rlike '线上班|线上\\+线下|特殊申请通道'
  and (teacher_if_student = 1 or (teacher_if_current = 1 and period_if_current = 1));


-- 统计例子数
DROP TABLE users;
CREATE TEMPORARY TABLE users AS
select a.title
     , b.class_sales_id
     , SUBSTR(c.name, 1, INSTR(c.name, REGEXP_EXTRACT(c.name, '^[\\u4e00-\\u9fa5]*([^\\u4e00-\\u9fa5]).*')) - 1) AS name
     , count(1)                                                                                                  as user_num
from xiaoe_class a
         left join vip_member b
                   on a.id = b.class_id
         left join ods.ods_place_sales c
                   on b.class_sales_id = c.id
group by a.title, b.class_sales_id, c.name;


-- 统计订单数
DROP TABLE orders;
CREATE TEMPORARY TABLE orders AS
select a.title,
       b.owner_id,
       sum(case
               when b.price >= 6580 and b.price < 21580 and a.end_date >= to_date(b.pay_time) then 1
               when price = 21580 and a.end_date >= to_date(b.pay_time) then 2
               else 0 end) as pay_num,  -- 销转期内的成单数
       sum(case
               when b.price >= 6580 and b.price < 21580 and a.end_date < to_date(b.pay_time) then 1
               when price = 21580 and a.end_date < to_date(b.pay_time) then 2
               else 0 end) as pay_num_1 -- 销转期外的成单数

from xiaoe_class a
         left join order_3z b
                   on a.resource_id = b.resource_id
group by a.title, b.owner_id;



-- 班主任明细数据
DROP TABLE detail;
CREATE TEMPORARY TABLE detail AS
select a.title                                              -- 期次
     , a.name                                               -- 班主任
     , a.user_num                                           --例子数
     , b.pay_num                                            --结课订单数
     , nvl(a.user_num / b.pay_num * 100, 0)     convert_rate--转化率
     , b.pay_num_1                                          --转化期外成单数
     , b.pay_num * 6580                      as pay_sum     --结课GMV
     , b.pay_num_1 * 6580                    as pay_sum_1   --结课外GMV
     , b.pay_num * 6580 + b.pay_num_1 * 6580 as pay_sum_all --总GMV
from users a
         left join orders b
                   on a.title = b.title and a.class_sales_id = b.owner_id;

-- 结果
INSERT
    OVERWRITE
    TABLE app.c_app_sanzhuan_hrbp_dashboard
    PARTITION
    (dt = '${datebuf}')
select '${datebuf}' AS d_date
        ,
       CASE GROUPING__ID
           WHEN 1 THEN '1_期次'
           WHEN 0 THEN '2_期次x班主任'
           END      AS grouptype ,title, name,
       sum(user_num),
       sum(pay_num),
       nvl(sum(pay_num)/sum(user_num) * 100, 0) as convert_rate,
       sum(pay_num_1),
       sum(pay_sum),
       sum(pay_sum_1),
       sum(pay_sum_all)
from detail
group by title, name
    GROUPING SETS (
       (title)
       , (title, name)
    )










