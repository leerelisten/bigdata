
CREATE TABLE IF NOT EXISTS dwd.dwd_example_consumption_details
(
    member_id      STRING COMMENT '用户ID',
    cat            STRING COMMENT '品类',
    ad_department  STRING COMMENT '广告部门',
    platform       STRING COMMENT '平台代码',
    platform_name  STRING COMMENT '平台名称',
    department     STRING COMMENT '销售部门',
    user_group     STRING COMMENT '用户组',
    h5_id          STRING COMMENT 'H5页面ID',
    p_source       STRING COMMENT '来源渠道',
    pos            STRING COMMENT '版位信息',
    price          STRING COMMENT '价格',
    mobile         STRING COMMENT '有无手机号(有手机号/无手机号)',
    link_type_v1   STRING COMMENT '链路类型-旧',
    link_type_v2   STRING COMMENT '链路类型-新',
    cost_id        STRING COMMENT '成本ID',
    cost_id2       STRING COMMENT '成本ID2',
    ad_id          STRING COMMENT '广告计划ID',
    sucai_id       STRING COMMENT '素材ID',
    created_at     STRING COMMENT '创建时间',
    wx_rel_status  STRING COMMENT '微信关系状态',
    goods_name     STRING COMMENT '商品名称',
    special_id     STRING COMMENT '期次ID',
    xe_id          STRING COMMENT '小鹅通ID',
    ifcollect      string COMMENT '是否收集问卷(1:是,0:否)',
    trade_state    STRING COMMENT '交易状态',
    member_status  INT COMMENT '会员状态',
    sales_id       STRING COMMENT '销售ID',
    sales_name     STRING COMMENT '销售名称',
    ifbuy          INT COMMENT '是否购买(1:是,0:否)',
    first_pay_time STRING COMMENT '首次支付时间',
    pay_num        float COMMENT '支付次数',
    pay_sum        float COMMENT '支付总金额',
    pay_num_D4     float COMMENT 'D4支付次数',
    pay_sum_D4     float COMMENT 'D4支付总金额',
    pay_num_D4_24h float COMMENT 'D4 24小时内支付次数',
    pay_sum_D4_24h float COMMENT 'D4 24小时内支付总金额',
    pay_num_D5     float COMMENT 'D5支付次数',
    pay_sum_D5     float COMMENT 'D5支付总金额',
    pay_num_D6     float COMMENT 'D6支付次数',
    pay_sum_D6     float COMMENT 'D6支付总金额',
    pay_num_D7     float COMMENT 'D7支付次数',
    pay_sum_D7     float COMMENT 'D7支付总金额',
    pay_num_D8     float COMMENT 'D8支付次数',
    pay_sum_D8     float COMMENT 'D8支付总金额',
    pay_num_D14    float COMMENT 'D14支付次数',
    pay_sum_D14    float COMMENT 'D14支付总金额',
    ifcome0        INT COMMENT '是否到课0(1:是,0:否)',
    ifok0          INT COMMENT '是否OK0(1:是,0:否)',
    ifcome1        INT COMMENT '是否到课1(1:是,0:否)',
    ifok1          INT COMMENT '是否OK1(1:是,0:否)',
    ifcome2        INT COMMENT '是否到课2(1:是,0:否)',
    ifok2          INT COMMENT '是否OK2(1:是,0:否)',
    ifcome3        INT COMMENT '是否到课3(1:是,0:否)',
    ifok3          INT COMMENT '是否OK3(1:是,0:否)',
    ifcome4        INT COMMENT '是否到课4(1:是,0:否)',
    ifok4          INT COMMENT '是否OK4(1:是,0:否)',
    ifcome5        INT COMMENT '是否到课5(1:是,0:否)',
    ifok5          INT COMMENT '是否OK5(1:是,0:否)',
    wx_active      INT COMMENT '微信活跃状态',
    collect_active INT COMMENT '问卷收集活跃状态',
    interest       INT COMMENT '太极兴趣度(1:高,0:低)',
    influence      INT COMMENT '太极紧迫度(1:高,0:低)',
    sex            INT COMMENT '性别(1:女,0:男)',
    age            INT COMMENT '年龄(1:50岁以上,0:50岁及以下)',
    cac            float COMMENT '客户获取成本',
    cac_real       float COMMENT '实际客户获取成本',
    d_date         STRING COMMENT '日期',
    is_get_ticket  STRING COMMENT '是否获得票(1否/2是)',
    is_abroad      string COMMENT '是否海外(1:是,0:否)'
)
    COMMENT '消费详情明细表'
    PARTITIONED BY (dt STRING COMMENT '日期分区')
    STORED AS ORC;


WITH cost AS
    (SELECT cost_id
          , d_date
          , SUM(cost)      AS cost
          , SUM(cost_real) AS cost_real
     FROM da.da_course_daily_cost_by_adid
     WHERE dt = '${datebuf}'
     GROUP BY cost_id
            , d_date)
   , users AS -- 20240914 本地推直播:直播一组,腾讯视频号直播付费流:直播二组,千川直播:直播三组
    (SELECT a.member_id
          , a.cat
          , a.ad_department
          , a.platform
          , a.platform_name
          , a.department
          , a.user_group
          , a.h5_id
          -- , a.p_source
          , a.pos
          , a.price
          , a.mobile
          , a.link_type_v1
          , a.link_type_v2
          , a.cost_id
          , a.cost_id2
          , a.ad_id
          , a.sucai_id
          , a.created_at
          , a.wx_rel_status
          , a.goods_name
          , a.special_id
          , a.xe_id
          , a.ifcollect
          , a.trade_state
          , a.member_status
          , a.sales_id
          , a.sales_name
          , a.ifbuy
          , a.first_pay_time
          , a.pay_num
          , a.pay_sum
          , a.pay_num_D4
          , a.pay_sum_D4
          , a.pay_num_D4_24h
          , a.pay_sum_D4_24h
          , a.pay_num_D5
          , a.pay_sum_D5
          , a.pay_num_D6
          , a.pay_sum_D6
          , a.pay_num_D7
          , a.pay_sum_D7
          , a.pay_num_D8
          , a.pay_sum_D8
          , a.pay_num_D14
          , a.pay_sum_D14
          , a.ifcome0
          , a.ifok0
          , a.ifcome1
          , a.ifok1
          , a.ifcome2
          , a.ifok2
          , a.ifcome3
          , a.ifok3
          , a.ifcome4
          , a.ifok4
          , a.ifcome5
          , a.ifok5
          , a.wx_active
          , a.collect_active
          , a.cac
          , a.cac_real
          , a.d_date
          , a.dt
          , a.is_get_ticket
          , a.is_abroad
     FROM dws.dws_sale_camping_user_day a
     WHERE a.dt = '${datebuf}'
       AND TO_DATE(CONCAT('20', SUBSTR(a.goods_name, 2, 2), '-', SUBSTR(a.goods_name, 4, 2), '-',
                          SUBSTR(a.goods_name, 6, 2))) >= '2024-05-01'
       AND a.created_at >= '2024-05-01'
       AND a.goods_name NOT LIKE '%测试%'
       AND (a.platform_name != '小糖私域' OR a.pos != '私域群活码') -- 20241113 剔除私域群活码
    )
   , user_num AS
    (SELECT cost_id2
          , d_date
          , COUNT(*) num
     FROM users
     WHERE dt = '${datebuf}'
       AND member_status = 1
       AND trade_state IN ('SUCCESS', 'PREPARE')
       AND sales_id > 0
       -- and to_date(created_at) >= '2024-07-01'
     GROUP BY cost_id2
            , d_date)
   , cac AS
    (SELECT a.cost_id
          , a.d_date
          , NVL(a.cost / b.num, 0)      AS cac
          , NVL(a.cost_real / b.num, 0) AS cac_real
     FROM cost a
              LEFT JOIN user_num b
                        ON a.cost_id = b.cost_id2
                            AND a.d_date = b.d_date)
   , user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
             , a.ad_department
             , a.platform
             , a.platform_name
             , a.department
             , a.user_group
             , a.h5_id
              --, a.p_source
             , a.pos
             , a.price
             , a.mobile
             , a.link_type_v1
             , CASE WHEN a.link_type_v2 = '新一键' THEN '新一键授权' ELSE a.link_type_v2 END AS link_type_v2
             , a.cost_id
             , a.cost_id2
             , a.ad_id
             , a.sucai_id
             , a.created_at
             , a.wx_rel_status
             , a.goods_name
             , a.special_id
             , a.xe_id
             , a.ifcollect
             , a.trade_state
             , a.member_status
             , a.sales_id
             , SPLIT(REGEXP_REPLACE(TRIM(a.sales_name), '[0-9]号|[0-9]', ''), '（')[0] AS sales_name
             , a.ifbuy
             , a.first_pay_time
             , a.pay_num
             , a.pay_sum
             , a.pay_num_D4
             , a.pay_sum_D4
             , a.pay_num_D4_24h
             , a.pay_sum_D4_24h
             , a.pay_num_D5
             , a.pay_sum_D5
             , a.pay_num_D6
             , a.pay_sum_D6
             , a.pay_num_D7
             , a.pay_sum_D7
             , a.pay_num_D8
             , a.pay_sum_D8
             , a.pay_num_D14
             , a.pay_sum_D14
             , a.ifcome0
             , a.ifok0
             , a.ifcome1
             , a.ifok1
             , a.ifcome2
             , a.ifok2
             , a.ifcome3
             , a.ifok3
             , a.ifcome4
             , a.ifok4
             , a.ifcome5
             , a.ifok5
             , a.wx_active
             , a.collect_active
             , g.interest
             , g.influence
             , g.sex
             , g.age
             , COALESCE(a.cac, d.cac, 0)                                             AS cac
             , COALESCE(a.cac_real, d.cac_real, 0)                                   AS cac_real
             , a.d_date
             , a.dt
             , a.is_get_ticket
             , a.is_abroad
        FROM users a
                 LEFT JOIN cac d -- 消耗数据
                           ON d.cost_id = a.cost_id2
                               AND d.d_date = a.d_date
                 LEFT JOIN (
            --20241126 康斌新增画像数据 添加去重
            SELECT member_id
                 , goods_name
                 , IF(
                        taiji_interest IN ('曾经线上或线下学习过太极，计划提高', '知道太极养生对健康的帮助，想学习太极'), 1,
                        0)                                                              interest  --太极兴趣度例子数
                 , IF(taiji_influence IN ('有时影响生活，需要调理改善', '影响不大，或已经找到改善方式'), 1, 0) influence --太极紧迫度例子数
                 , IF(sex = '女', 1, 0)                                              sex       --女性例子数
                 , IF(age_level IN ('56-60岁', '66-70岁', '71岁及以上', '51-55岁', '61-65岁'), 1,
                      0)                                                            age       --50岁以上例子数
            FROM app.c_app_course_xt_user_profile
            WHERE dt = '${datebuf}'
              AND goods_name RLIKE '太极'--限制太极
            GROUP BY member_id
                   , goods_name
                   , IF(
                        taiji_interest IN ('曾经线上或线下学习过太极，计划提高', '知道太极养生对健康的帮助，想学习太极'), 1,
                        0)
                   , IF(taiji_influence IN ('有时影响生活，需要调理改善', '影响不大，或已经找到改善方式'), 1, 0)
                   , IF(sex = '女', 1, 0)
                   , IF(age_level IN ('56-60岁', '66-70岁', '71岁及以上', '51-55岁', '61-65岁'), 1, 0)) g
                           ON a.member_id = g.member_id
                               AND a.goods_name = g.goods_name
    )


INSERT OVERWRITE TABLE dwd.dwd_example_consumption_details PARTITION (dt = '${datebuf}')
SELECT
    member_id
     ,cat
     ,ad_department
     ,platform
     ,platform_name
     ,department
     ,user_group
     ,h5_id
     ,'' AS p_source
     ,pos
     ,price
     ,mobile
     ,link_type_v1
     ,link_type_v2
     ,cost_id
     ,cost_id2
     ,ad_id
     ,sucai_id
     ,created_at
     ,wx_rel_status
     ,goods_name
     ,special_id
     ,xe_id
     ,ifcollect
     ,trade_state
     ,member_status
     ,sales_id
     ,sales_name
     ,ifbuy
     ,first_pay_time
     ,pay_num
     ,pay_sum
     ,pay_num_D4
     ,pay_sum_D4
     ,pay_num_D4_24h
     ,pay_sum_D4_24h
     ,pay_num_D5
     ,pay_sum_D5
     ,pay_num_D6
     ,pay_sum_D6
     ,pay_num_D7
     ,pay_sum_D7
     ,pay_num_D8
     ,pay_sum_D8
     ,pay_num_D14
     ,pay_sum_D14
     ,ifcome0
     ,ifok0
     ,ifcome1
     ,ifok1
     ,ifcome2
     ,ifok2
     ,ifcome3
     ,ifok3
     ,ifcome4
     ,ifok4
     ,ifcome5
     ,ifok5
     ,wx_active
     ,collect_active
     ,interest
     ,influence
     ,sex
     ,age
     ,cac
     ,cac_real
     ,d_date
     ,is_get_ticket
     ,is_abroad
FROM user_info
;


