CREATE TABLE IF NOT EXISTS app.c_app_course_daily_baidusucai_dashboard_repartition
(
    d_date              string COMMENT '日期',
    group_type          string COMMENT '分组类型',
    goods_name          string COMMENT '期次',
    cat                 string COMMENT '品类',
    ad_department       string COMMENT '投放部门',
    platform_name       string COMMENT '渠道',
    pos                 string COMMENT '版位',
    price               string COMMENT '价格',
    link_type_v2        string COMMENT '链路类型(新)',
    mobile              string COMMENT '收集手机号',
    agent               string COMMENT '代理',
    cost_id             string COMMENT '账户id',
    ad_id               string COMMENT '计划id',
    sucai_id            string COMMENT '创意id/单元id',
    sucai_name          string COMMENT '素材名称',
    cost                float COMMENT '账面消耗(元)',
    cost_real           float COMMENT '实际消耗(元)',
    submit_num          int COMMENT '表单填写例子数(个)',
    payment_num         int COMMENT '支付成功例子数(个)',
    user_num            int COMMENT '例子数(个)',
    wx_num              int COMMENT '加微例子数(个)',
    collect_num         int COMMENT '填问卷例子数(个)',
    pay_num_D4          float COMMENT 'D4正价课订单数(单)',
    pay_sum_D4          float COMMENT 'D4正价课GMV(元)',
    roi_D4              float COMMENT 'D4ROI',
    pay_user_num        int COMMENT '购买正价课例子数(个)',
    pay_num             float COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    roi                 float COMMENT 'ROI=正价课GMV/实际支出',
    conv_rate           float COMMENT '转化率=正价课订单数/例子数(%)',
    cac                 float COMMENT 'CAC=实际支出/例子数(元/个)',
    wx_rate             float COMMENT '加微率=加微例子数/例子数(%)',
    wx_active_rate      float COMMENT '主动加微率=主动加微例子数/例子数(%)',
    collect_rate        float COMMENT '问卷率=填问卷例子数/例子数(%)',
    collect_active_rate float COMMENT '主动问卷率=主动填问卷例子数/例子数(%)',
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
    impression          float COMMENT '展现(次)',
    click               float COMMENT '点击(次)',
    ctr                 float COMMENT '点击率(%)',
    cpc                 float COMMENT '平均点击价格(元/次)',
    ocpcTargetTrans     float COMMENT '目标转化量(次)',
    ocpcTargetTransCPC  float COMMENT '目标转化成本(元/次)',
    completePlayCount   float COMMENT '播放完成数(次)',
    completePlayCost    float COMMENT '播放完成成本(元/次)',
    completePlayRatio   float COMMENT '播放完成率(%)',
    playCount1          float COMMENT '播放至25%及以上的次数(次)',
    playCount2          float COMMENT '播放至50%及以上的次数(次)',
    playCount3          float COMMENT '播放至75%及以上的次数(次)',
    playCount4          float COMMENT '播放至100%的次数(次)',
    avgPlayTime         float COMMENT '平均播放时长(s/次)',
    manualPlayCount     float COMMENT '主动播放数(次)',
    autoPlayCount       float COMMENT '自动播放数(次)',
    effectivePlayCount  float COMMENT '有效播放数(次)',
    permission          string COMMENT '权限用户'
)
    COMMENT '培训主题数仓-百度素材报表-重新分区'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_baidusucai_dashboard_repartition';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.max.created.files=1000000;


INSERT OVERWRITE TABLE app.c_app_course_daily_baidusucai_dashboard_repartition PARTITION (dt)
SELECT a.d_date
     , '日' as group_type
     , null as goods_name
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
     , a.sucai_id
     , a.sucai_name
     , a.cost
     , a.cost_real
     , a.submit_num
     , a.payment_num
     , a.user_num
     , a.wx_num
     , a.collect_num
     , a.pay_num_D4
     , a.pay_sum_D4
     , a.roi_D4
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
     , a.impression
     , a.click
     , a.ctr
     , a.cpc
     , a.ocpcTargetTrans
     , a.ocpcTargetTransCPC
     , a.completePlayCount
     , a.completePlayCost
     , a.completePlayRatio
     , a.playCount1
     , a.playCount2
     , a.playCount3
     , a.playCount4
     , a.avgPlayTime
     , a.manualPlayCount
     , a.autoPlayCount
     , a.effectivePlayCount
     , CONCAT_WS(',', COLLECT_SET(pm.emails)) AS permission
     , a.d_date AS dt
FROM app.c_app_course_daily_baidusucai_dashboard a
         LEFT JOIN (SELECT is_abroad
                         , cat
                         , platform_name
                         , pos
                         , ad_department
                         , sale_department
                         , sop_type
                         , emails
                    FROM dws.dws_report_permission_day
                    WHERE dt = '${datebuf}'
                      AND report_id = '1427') pm
                   ON (pm.ad_department = '全部' OR a.ad_department = pm.ad_department)
WHERE a.dt = '${datebuf}'
  AND a.pos IN ('百度搜索', '百度信息流','百度信息流北京')
  AND a.d_date >= '2024-08-21'
GROUP BY a.d_date
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
       , a.sucai_id
       , a.sucai_name
       , a.cost
       , a.cost_real
       , a.submit_num
       , a.payment_num
       , a.user_num
       , a.wx_num
       , a.collect_num
       , a.pay_num_D4
       , a.pay_sum_D4
       , a.roi_D4
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
       , a.impression
       , a.click
       , a.ctr
       , a.cpc
       , a.ocpcTargetTrans
       , a.ocpcTargetTransCPC
       , a.completePlayCount
       , a.completePlayCost
       , a.completePlayRatio
       , a.playCount1
       , a.playCount2
       , a.playCount3
       , a.playCount4
       , a.avgPlayTime
       , a.manualPlayCount
       , a.autoPlayCount
       , a.effectivePlayCount
       , a.d_date
union all
select a.d_date
       ,'期次'
       ,a.period
       ,a.cat
       ,a.ad_department
       ,a.platform_name
       ,a.pos
       ,a.price
       ,a.link_type_v2
       ,a.mobile
       ,a.agent
       ,a.cost_id
       ,a.ad_id
       ,a.sucai_id
       ,a.sucai_name
       ,a.cost
       ,a.cost_real
       ,a.submit_num
       ,a.payment_num
       ,a.user_num
       ,a.wx_num
       ,a.collect_num
       ,a.pay_num_D4
       ,a.pay_sum_D4
       ,a.roi_D4
       ,a.pay_user_num
       ,a.pay_num
       ,a.pay_sum
       ,a.roi
       ,a.conv_rate
       ,a.cac
       ,a.wx_rate
       ,a.wx_active_rate
       ,a.collect_rate
       ,a.collect_active_rate
       ,a.ifcome0_rate
       ,a.ifok0_rate
       ,a.ifcome1_rate
       ,a.ifok1_rate
       ,a.ifcome2_rate
       ,a.ifok2_rate
       ,a.ifcome3_rate
       ,a.ifok3_rate
       ,a.ifcome4_rate
       ,a.ifok4_rate
       ,a.ifcome5_rate
       ,a.ifok5_rate
       ,a.wx_active_num
       ,a.collect_active_num
       ,a.ifcome0
       ,a.ifok0
       ,a.ifcome1
       ,a.ifok1
       ,a.ifcome2
       ,a.ifok2
       ,a.ifcome3
       ,a.ifok3
       ,a.ifcome4
       ,a.ifok4
       ,a.ifcome5
       ,a.ifok5
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       ,null
       , CONCAT_WS(',', COLLECT_SET(pm.emails)) AS permission
       , a.d_date AS dt
FROM app.c_app_course_period_baidusucai_dashboard a
         LEFT JOIN (SELECT is_abroad
                         , cat
                         , platform_name
                         , pos
                         , ad_department
                         , sale_department
                         , sop_type
                         , emails
                    FROM dws.dws_report_permission_day
                    WHERE dt = '${datebuf}'
                      AND report_id = '1427') pm
                   ON (pm.ad_department = '全部' OR a.ad_department = pm.ad_department)
WHERE a.dt = '${datebuf}'
  AND a.pos IN ('百度搜索', '百度信息流','百度信息流北京')
GROUP BY a.d_date
       , a.cat
       , a.period
       , a.ad_department
       , a.platform_name
       , a.pos
       , a.price
       , a.link_type_v2
       , a.mobile
       , a.agent
       , a.cost_id
       , a.ad_id
       , a.sucai_id
       , a.sucai_name
       , a.cost
       , a.cost_real
       , a.submit_num
       , a.payment_num
       , a.user_num
       , a.wx_num
       , a.collect_num
       , a.pay_num_D4
       , a.pay_sum_D4
       , a.roi_D4
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
       , a.d_date
