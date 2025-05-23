SET mapred.job.name="c_app_sanzhuan_daily_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_sanzhuan_daily_dashboard
(
    d_date              STRING COMMENT '数据日期',
    title               STRING COMMENT '期次',
    name                STRING COMMENT '班主任昵称',
    user_num               INT COMMENT '配额(个)',
    pay_date            STRING COMMENT '转化日期',
    pay_num1               INT COMMENT '炼神营_当日(单)',
    pay_num2               INT COMMENT '炼神营+线下_当日(单)',
    pay_num3               INT COMMENT '补差价_当日(单)',
    pay_all                INT COMMENT '单数_当日(单)',
    conv_rate_day        FLOAT COMMENT '转化率_当日(%)',
    gmv_day              FLOAT COMMENT 'GMV_当日(元)',
    pay_all_rolling        INT COMMENT '总单数(单)',
    conv_rate_rolling    FLOAT COMMENT '总转化率(%)',
    gmv_rolling          FLOAT COMMENT '总GMV(元)',
    stu_num_rolling        INT COMMENT '学员数(个)',
    pay_all_growth         INT COMMENT '当日单数环比增长(单)',
    conv_rate_day_growth FLOAT COMMENT '当日转化率环比增长(%)',
    gmv_day_growth       FLOAT COMMENT '当日GMV环比增长(元)'

)
    COMMENT '三转日数据报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_sanzhuan_daily_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;


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
         WHERE order_state <> 10 --剔除全额退款 -- 订单状态成功 1成功 11部分退款 10全部退款 5审批中 2审批失败
           AND delay_status = 0 --延期选正常
     )a LEFT JOIN change_account_data b  --关联换号数据 排除掉换号后对应的例子 保留换号前例子
                  ON a.xiaoe_member_id = b.relate_member_id
                      AND a.xiaoe_order_resource_id = b.special_id
WHERE b.relate_member_id IS NULL
  AND b.special_id IS NULL
;

-- 订单信息
DROP TABLE xiaoe_order;
CREATE TEMPORARY TABLE xiaoe_order AS
select a.*, c.goods_name
from ods.ods_xiaoe_order_dt a
         left join ods.ods_xiaoe_special c
                   on a.resource_id = c.xe_id
where a.dt = '${datebuf}'
  and a.resource_type in ( '8', '100008')
  and a.order_state in (1,11)
  and a.xiaoe_order_type <> '开放API导入订单'
  and a.price > '0'
  and a.xe_app_id = 'appcafhwq5q8671'
  and c.goods_name rlike '线上班|线上\\+线下|特殊申请通道';

--每一期次对应的全部订单数据
--注：三转这里订单不用处理，这里用期次去判断三转订单。故不做换号处理
DROP TABLE order_all;
CREATE TEMPORARY TABLE order_all AS
select b.owner_id, b.price, b.goods_name, b.pay_time,
       a.title,
       a.resource_id,
       a.end_date,
       a.start_date,
       case when a.end_date<to_date(b.pay_time) then '补单' else to_date(b.pay_time) end as pay_date
from xiaoe_class a
         left join xiaoe_order b
                   on a.resource_id = b.owner_class and a.start_date<=to_date(b.pay_time);

-- 统计例子数
DROP TABLE users;
CREATE TEMPORARY TABLE users AS
select a.title
     , b.class_sales_id
     , c.nickname AS name
     , count(1)                                                                                                  as user_num
from xiaoe_class a
         left join vip_member b
                   on a.id = b.class_id
         left join ods.ods_place_sales c
                   on b.class_sales_id = c.id
group by a.title, b.class_sales_id, c.nickname;

-- 统计订单数
DROP TABLE orders;
CREATE TEMPORARY TABLE orders AS
select title,
       owner_id,
       pay_date,
       sum(case when price = 6580 then 1 else 0 end) as pay_num1,  -- 炼神营
       sum(case when price = 21580 then 1 else 0 end) as pay_num2,  -- 炼神营+线下
       sum(case when price = 15000 then 1 else 0 end) as pay_num3  -- 补差价
from order_all
group by title, owner_id,pay_date;

-- 班主任、天维度明细数据
DROP TABLE detail;
CREATE TEMPORARY TABLE detail AS
select a.title                                              -- 期次
     , a.name                                               -- 班主任
     , a.user_num                                           --例子数
     , b.pay_date                                           -- 支付日期
     , b.pay_num1                                            --炼神营
     , b.pay_num2                                           -- 炼神营+线下
     , b.pay_num3                                           -- 补差价
     , b.pay_num1+b.pay_num2*2+b.pay_num3 as pay_all    --当日单数
     , (b.pay_num1+b.pay_num2*2+b.pay_num3)/user_num*100  as conv_rate_day  --当日转化率
     , b.pay_num1*6580+b.pay_num2*21580+b.pay_num3*15000 as gmv_day--当日GMV
     , case when pay_date='补单' then NULL
            else sum(b.pay_num1+b.pay_num2*2+b.pay_num3) over (partition by a.title,a.name order by b.pay_date)
    end as pay_all_rolling --总单数_滚动数据
     , case when pay_date='补单' then NULL
            else sum(b.pay_num1+b.pay_num2*2+b.pay_num3) over (partition by a.title,a.name order by b.pay_date)
           end/a.user_num*100 as conv_rate_rolling --总转化率_滚动数据
     , case when pay_date='补单' then NULL
            else sum(b.pay_num1*6580+b.pay_num2*21580+b.pay_num3*15000) over (partition by a.title,a.name order by b.pay_date)
           end/a.user_num as gmv_rolling --总GMV_滚动数据
     , case when pay_date='补单' then NULL
            else sum(b.pay_num1+b.pay_num2) over (partition by a.title,a.name order by b.pay_date)
    end as stu_num_rolling --学员数_滚动数据
     , case when pay_date='补单' or lag(b.pay_num1+b.pay_num2*2+b.pay_num3) over (partition by a.title,a.name order by b.pay_date) is null then null
            else b.pay_num1+b.pay_num2*2+b.pay_num3-lag(b.pay_num1+b.pay_num2*2+b.pay_num3) over (partition by a.title,a.name order by b.pay_date)
    end as pay_all_growth --当日单数环比增长
     , case when pay_date='补单' or lag(b.pay_num1+b.pay_num2*2+b.pay_num3) over (partition by a.title,a.name order by b.pay_date) is null then null
            else (b.pay_num1+b.pay_num2*2+b.pay_num3-lag(b.pay_num1+b.pay_num2*2+b.pay_num3) over (partition by a.title,a.name order by b.pay_date) )/user_num*100
    end as conv_rate_day_growth  --当日转化率环比增长
     , case when pay_date='补单' or lag(b.pay_num1*6580+b.pay_num2*21580+b.pay_num3*15000) over (partition by a.title,a.name order by b.pay_date) is null then null
            else b.pay_num1*6580+b.pay_num2*21580+b.pay_num3*15000-lag(b.pay_num1*6580+b.pay_num2*21580+b.pay_num3*15000) over (partition by a.title,a.name order by b.pay_date)
    end as gmv_day_growth --当日GMV环比增长
from users a
         left join orders b
                   on a.title = b.title and a.class_sales_id = b.owner_id
union all
select a.title                                              -- 期次
     , a.name                                               -- 班主任
     , a.user_num                                           --例子数
     , '转化期内汇总' as pay_date                                           -- 支付日期
     , sum(b.pay_num1) as pay_num1                                          --炼神营
     , sum(b.pay_num2) as pay_num2                                        -- 炼神营+线下
     , sum(b.pay_num3) as pay_num3                                        -- 补差价
     , sum(b.pay_num1+b.pay_num2*2+b.pay_num3) as pay_all    --当日单数
     , sum(b.pay_num1+b.pay_num2*2+b.pay_num3)/user_num*100  as conv_rate_day  --当日转化率
     , sum(b.pay_num1*6580+b.pay_num2*21580+b.pay_num3*15000) as gmv_day--当日GMV
     , CAST(NULL AS INT) as pay_all_rolling --总单数_滚动数据
     , CAST(NULL AS FLOAT) as conv_rate_rolling --总转化率_滚动数据
     , CAST(NULL AS FLOAT) as gmv_rolling --总GMV_滚动数据
     , CAST(NULL AS INT) as stu_num_rolling --学员数_滚动数据
     , CAST(NULL AS INT) as pay_all_growth --当日单数环比增长
     , CAST(NULL AS FLOAT) as conv_rate_day_growth  --当日转化率环比增长
     , CAST(NULL AS FLOAT) as gmv_day_growth --当日GMV环比增长
from users a
         left join orders b
                   on a.title = b.title and a.class_sales_id = b.owner_id
where b.pay_date<>'补单'
group by a.title,a.name,a.user_num;

-- 结果
INSERT
    OVERWRITE
    TABLE app.c_app_sanzhuan_daily_dashboard
    PARTITION
(dt = '${datebuf}')
select '${datebuf}' AS d_date
     ,title
     ,name
     ,user_num
     ,pay_date
     ,pay_num1
     ,pay_num2
     ,pay_num3
     ,pay_all
     ,conv_rate_day
     ,gmv_day
     ,pay_all_rolling
     ,conv_rate_rolling
     ,gmv_rolling
     ,stu_num_rolling
     ,pay_all_growth
     ,conv_rate_day_growth
     ,gmv_day_growth
from detail;