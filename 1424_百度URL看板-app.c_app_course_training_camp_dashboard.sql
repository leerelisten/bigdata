-- 24.9.23 袁俊君需求：目前百度搜索账户分析投放效果数据，只能分析到账户层级、计划，单元、关键词（搜索）、搜索词（搜索）、创意（搜索/信息流）等细颗粒度的效果数据无法分析，在学员信息表上新增需求字段后，可通过URL字段解析出细颗粒度的投放效果数据；
-- 数据范围：仅百度渠道


SET mapred.job.name="c_app_course_training_camp_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_training_camp_dashboard
(
    d_date              string COMMENT '领课日期',
    member_id           string COMMENT '用户ID',
    contact_ex_nickname string COMMENT '用户名',
    mobile              string COMMENT '联系电话',
    intention_level     string COMMENT '意向等级 1无 2甲、3已、4丙',
    goods_name          string COMMENT '期次',
    created_at          string COMMENT '当前领课时间',
    pay_num             float COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    department          string COMMENT '销售部门',
    user_group          string COMMENT '销售组',
    sales_name          string COMMENT '销售姓名',
    wx_relation_status  string COMMENT '微信关系',
    is_get_ticket       string COMMENT '是否已领券',
    ifbuy               string COMMENT '最大销售状态',
    xe_id               string COMMENT '小鹅通ID',
    unionid             string COMMENT 'unionid',
    link_type_v2        string COMMENT '链路类型',
    price               string COMMENT '价格',
    platform_name       string COMMENT '渠道',
    pos                 string COMMENT '版位',
    cost_id             string COMMENT '账户',
    report_link         string COMMENT '投放URL'
)
    COMMENT '培训主题数仓-CRM训练营学员详情看板(仅百度)'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_training_camp_dashboard';
SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;
-- 开启动态分区
SET hive.exec.dynamic.partition=true;
-- 设置非严格模式
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.concurrency=false;
-- 设置最大分区数，默认为100个
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

INSERT
    OVERWRITE
    TABLE app.c_app_course_training_camp_dashboard
    PARTITION
    (dt)
SELECT TO_DATE(ui.created_at)                         AS d_date             --领课日期
     , ui.member_id                                                         --用户id
     , xm.contact_ex_nickname                                               --用户名
     , CONCAT(SUBSTR(xm.mobile, 1, 3)
    , '****'
    , SUBSTR(xm.mobile, 8, 4))                        AS mobile             --联系电话
     -- 意向等级 1无 2甲、3已、4丙
     , CASE xm.intention_level
           WHEN 1 THEN '无'
           WHEN 2 THEN '甲'
           WHEN 3 THEN '乙'
           WHEN 4 THEN '丙'
           ELSE xm.intention_level
    END                                               AS intention_level    --意向等级
     , ui.goods_name                                                        --期次
     , ui.created_at                                                        --领课时间
     , NVL(bc.pay_num, 0)                             AS pay_num            --正价课订单数
     , NVL(bc.pay_sum, 0)                             AS pay_sum            --正价课GMV
     , ui.department                                                        --销售部门
     , ui.user_group                                                        --销售组
     , ui.sales_name                                                        --销售姓名
     -- wx_relation_status,1未添加微信,2已添加微信,3单向好友
     , CASE xm.wx_relation_status
           WHEN 1 THEN '未添加微信'
           WHEN 2 THEN '已添加微信'
           WHEN 3 THEN '用户删除'
           WHEN 4 THEN '内部删除'
           ELSE xm.wx_relation_status
    END                                               AS wx_relation_status --微信关系
     , ui.is_get_ticket                                                     --是否已领券
     , IF(bc.user_id IS NOT NULL, '已转化', '无转化') AS ifbuy              --转化状态
     , xm.xe_id                                                             -- 小鹅通ID
     , xm.unionid
     , ui.link_type_v2                                                      --链路类型
     , ui.price                                                             --价格
     , ui.platform_name                                                     --渠道
     , ui.pos                                                               --版位
     , ui.cost_id                                                           --账户
     , ui.report_link                                                       --投放URL
     , TO_DATE(ui.created_at)                         AS dt
FROM (SELECT *
      FROM dw.dwd_xt_user
      WHERE dt = '${datebuf}'
        AND platform_name = '百度'
        AND TO_DATE(CONCAT('20', SUBSTR(goods_name, 2, 2), '-', SUBSTR(goods_name, 4, 2), '-',
                           SUBSTR(goods_name, 6, 2))) >= '2024-05-01'
        AND created_at >= '2024-05-01'
        --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--         AND goods_name NOT LIKE '%测试%'
        AND member_status = 1
        AND trade_state IN ('SUCCESS', 'PREPARE')
        AND sales_id > 0
        --11.13日新增剔除小糖私域-私域群活码链路
        AND (platform_name != '小糖私域' OR pos != '私域群活码')) ui
         LEFT JOIN ods.ods_xiaoe_member xm
                   ON ui.member_id = xm.id
         LEFT JOIN (SELECT * FROM dws.dws_sale_buy_course_day WHERE dt = '${datebuf}') bc
                   ON ui.xe_id = bc.user_id
                       AND ui.special_id = bc.owner_class;