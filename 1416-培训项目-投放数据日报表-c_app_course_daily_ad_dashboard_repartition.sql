CREATE TABLE IF NOT EXISTS app.c_app_course_daily_ad_dashboard_repartition
(
    d_date              string COMMENT '日期',
    grouptype           string COMMENT '分组类型',
    is_abroad           varchar(20) COMMENT '海内外',
    cat                 string COMMENT '品类',
    ad_department       string COMMENT '投放部门',
    platform_name       STRING COMMENT '渠道',
    pos                 string COMMENT '版位',
    price               string COMMENT '价格',
    link_type_v2        string COMMENT '链路类型(新)',
    mobile              string COMMENT '收集手机号',
    agent               string COMMENT '代理',
    cost_id             string COMMENT '账户id',
    ad_id               string COMMENT '计划id',
    cost                float COMMENT '账面消耗(元)',
    cost_real           float COMMENT '实际消耗(元)',
    submit_num          int COMMENT '表单填写例子数(个)',
    payment_num         int COMMENT '支付成功例子数(个)',
    user_num            int COMMENT '例子数(个)',
    wx_num              int COMMENT '加微例子数(个)',
    collect_num         int COMMENT '填问卷例子数(个)',
    pay_user_num        int COMMENT '购买正价课例子数(个)',
    pay_num             float COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    roi                 float COMMENT 'ROI',
    conv_rate           float COMMENT '转化率(%)',
    cac                 float COMMENT 'CAC(元/个)',
    wx_rate             float COMMENT '加微率(%)',
    wx_active_rate      float COMMENT '主动加微率(%)',
    collect_rate        float COMMENT '问卷率(%)',
    collect_active_rate float COMMENT '主动问卷率(%)',
    ifcome0_rate        float COMMENT '导学课到课率(%)',
    ifok0_rate          float COMMENT '导学课完课率(%)',
    ifcome1_rate        float COMMENT 'D1到课率(%)',
    ifok1_rate          float COMMENT 'D1完课率(%)',
    ifcome2_rate        float COMMENT 'D2到课率(%)',
    ifok2_rate          float COMMENT 'D2完课率(%)',
    ifcome3_rate        float COMMENT 'D3到课率(%)',
    ifok3_rate          float COMMENT 'D3完课率(%)',
    ifcome4_rate        float COMMENT 'D4到课率(%)',
    ifok4_rate          float COMMENT 'D4完课率(%)',
    ifcome5_rate        float COMMENT 'D5到课率(%)',
    ifok5_rate          float COMMENT 'D5完课率(%)',
    wx_active_num       int COMMENT '主动加微例子数(个)',
    collect_active_num  int COMMENT '主动填问卷例子数(个)',
    ifcome0             int COMMENT '导学课到课例子数(个)',
    ifok0               int COMMENT '导学课完课例子数(个)',
    ifcome1             int COMMENT 'D1到课例子数(个)',
    ifok1               int COMMENT 'D1完课例子数(个)',
    ifcome2             int COMMENT 'D2到课例子数(个)',
    ifok2               int COMMENT 'D2完课例子数(个)',
    ifcome3             int COMMENT 'D3到课例子数(个)',
    ifok3               int COMMENT 'D3完课例子数(个)',
    ifcome4             int COMMENT 'D4到课例子数(个)',
    ifok4               int COMMENT 'D4完课例子数(个)',
    ifcome5             int COMMENT 'D5到课例子数(个)',
    ifok5               int COMMENT 'D5完课例子数(个)',
    permission          string COMMENT '权限用户'
)
    COMMENT '培训主题数仓-投放数据日报表-重新分区'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_ad_dashboard_repartition';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.max.created.files=1000000;


INSERT OVERWRITE TABLE app.c_app_course_daily_ad_dashboard_repartition PARTITION (dt)
SELECT a.d_date
     , a.grouptype
     , a.is_abroad
     , a.cat
     , a.ad_department
     , a.platform_name
     , a.pos
     , a.price
     , a.link_type_v2
     , a.mobile
     , a.agent
     , a.cost_id
     , a.ad_id
     , a.cost
     , a.cost_real
     , a.submit_num
     , a.payment_num
     , a.user_num
     , a.wx_num
     , a.collect_num
     , a.pay_user_num
     , a.pay_num
     , a.pay_sum
     , a.roi
     , a.conv_rate
     , a.cac
     , a.wx_rate
     , a.wx_active_rate
     , a.collect_rate
     , a.collect_active_rate
     , a.ifcome0_rate
     , a.ifok0_rate
     , a.ifcome1_rate
     , a.ifok1_rate
     , a.ifcome2_rate
     , a.ifok2_rate
     , a.ifcome3_rate
     , a.ifok3_rate
     , a.ifcome4_rate
     , a.ifok4_rate
     , a.ifcome5_rate
     , a.ifok5_rate
     , a.wx_active_num
     , a.collect_active_num
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
     , CONCAT_WS(',', COLLECT_SET(pm.emails)) AS permission
     , a.d_date                               AS dt
FROM (SELECT *
      FROM app.c_app_course_daily_ad_dashboard
      WHERE dt = '${datebuf}'
        AND d_date >= '2024-05-01') a
         -- 24.12.30修改自动化权限代码
         LEFT JOIN
     (SELECT is_abroad
           , cat
           , platform_name
           , pos
           , ad_department
           , sale_department
           , sop_type
           , emails
      FROM dws.dws_report_permission_day
      WHERE dt = '${datebuf}'
        AND report_id = '1416') pm
     ON (pm.is_abroad = '全部' OR a.is_abroad = pm.is_abroad)
         AND (pm.cat = '全部' OR a.cat = pm.cat)
         AND (pm.platform_name = '全部' OR a.platform_name = pm.platform_name)
         AND (pm.pos = '全部' OR a.pos = pm.pos)
         AND (pm.ad_department = '全部' OR a.ad_department = pm.ad_department)


GROUP BY a.d_date, a.grouptype, a.is_abroad, a.cat, a.ad_department, a.platform_name, a.pos, a.price, a.link_type_v2
       , a.mobile, a.agent, a.cost_id, a.ad_id, a.cost, a.cost_real, a.submit_num, a.payment_num, a.user_num, a.wx_num
       , a.collect_num, a.pay_user_num, a.pay_num, a.pay_sum, a.roi, a.conv_rate, a.cac, a.wx_rate, a.wx_active_rate
       , a.collect_rate, a.collect_active_rate, a.ifcome0_rate, a.ifok0_rate, a.ifcome1_rate, a.ifok1_rate
       , a.ifcome2_rate, a.ifok2_rate, a.ifcome3_rate, a.ifok3_rate, a.ifcome4_rate, a.ifok4_rate, a.ifcome5_rate
       , a.ifok5_rate, a.wx_active_num, a.collect_active_num, a.ifcome0, a.ifok0, a.ifcome1, a.ifok1, a.ifcome2, a.ifok2
       , a.ifcome3, a.ifok3, a.ifcome4, a.ifok4, a.ifcome5, a.ifok5, a.dt