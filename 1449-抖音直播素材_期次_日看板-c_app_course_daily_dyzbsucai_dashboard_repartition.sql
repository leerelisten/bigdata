CREATE TABLE IF NOT EXISTS app.c_app_course_daily_dyzbsucai_dashboard_repartition
(
    d_date                     string COMMENT '日期',
    group_type                 string COMMENT '分组类型',
    goods_name                 string COMMENT '期次',
    cat                        string COMMENT '品类',
    ad_department              string COMMENT '投放部门',
    platform_name              string COMMENT '渠道',
    pos                        string COMMENT '版位',
    price                      string COMMENT '价格',
    link_type_v2               string COMMENT '链路类型(新)',
    mobile                     string COMMENT '收集手机号',
    agent                      string COMMENT '代理',
    cost_id                    string COMMENT '账户id',
    ad_id                      string COMMENT '计划id',
    sucai_id                   string COMMENT '素材id',
    sucai_name                 string COMMENT '素材名称',
    cost                       float COMMENT '账面消耗(元)',
    cost_real                  float COMMENT '实际消耗(元)',
    submit_num                 int COMMENT '表单填写例子数(个)',
    payment_num                int COMMENT '支付成功例子数(个)',
    user_num                   int COMMENT '例子数(个)',
    wx_num                     int COMMENT '加微例子数(个)',
    collect_num                int COMMENT '填问卷例子数(个)',
    pay_num_D4                 float COMMENT 'D4正价课订单数(单)',
    pay_sum_D4                 float COMMENT 'D4正价课GMV(元)',
    roi_D4                     float COMMENT 'D4ROI',
    pay_user_num               int COMMENT '购买正价课例子数(个)',
    pay_num                    float COMMENT '正价课订单数(单)',
    pay_sum                    float COMMENT '正价课GMV(元)',
    roi                        float COMMENT 'ROI=正价课GMV/实际支出',
    conv_rate                  float COMMENT '转化率=正价课订单数/例子数(%)',
    cac                        float COMMENT 'CAC=实际支出/例子数(元/个)',
    wx_rate                    float COMMENT '加微率=加微例子数/例子数(%)',
    wx_active_rate             float COMMENT '主动加微率=主动加微例子数/例子数(%)',
    collect_rate               float COMMENT '问卷率=填问卷例子数/例子数(%)',
    collect_active_rate        float COMMENT '主动问卷率=主动填问卷例子数/例子数(%)',
    ifcome0_rate               float COMMENT '导学课到课率(%)',
    ifok0_rate                 float COMMENT '导学课完课率(%)',
    ifcome1_rate               float COMMENT 'D1到课率(%)',
    ifok1_rate                 float COMMENT 'D1完课率(%)',
    ifcome2_rate               float COMMENT 'D2到课率(%)',
    ifok2_rate                 float COMMENT 'D2完课率(%)',
    ifcome3_rate               float COMMENT 'D3到课率(%)',
    ifok3_rate                 float COMMENT 'D3完课率(%)',
    ifcome4_rate               float COMMENT 'D4到课率(%)',
    ifok4_rate                 float COMMENT 'D4完课率(%)',
    ifcome5_rate               float COMMENT 'D5到课率(%)',
    ifok5_rate                 float COMMENT 'D5完课率(%)',
    wx_active_num              int COMMENT '主动加微例子数(个)',
    collect_active_num         int COMMENT '主动填问卷例子数(个)',
    ifcome0                    int COMMENT '导学课到课例子数(个)',
    ifok0                      int COMMENT '导学课完课例子数(个)',
    ifcome1                    int COMMENT 'D1到课例子数(个)',
    ifok1                      int COMMENT 'D1完课例子数(个)',
    ifcome2                    int COMMENT 'D2到课例子数(个)',
    ifok2                      int COMMENT 'D2完课例子数(个)',
    ifcome3                    int COMMENT 'D3到课例子数(个)',
    ifok3                      int COMMENT 'D3完课例子数(个)',
    ifcome4                    int COMMENT 'D4到课例子数(个)',
    ifok4                      int COMMENT 'D4完课例子数(个)',
    ifcome5                    int COMMENT 'D5到课例子数(个)',
    ifok5                      int COMMENT 'D5完课例子数(个)',
    show_cnt                   float COMMENT '曝光量(次)',
    click_cnt                  float COMMENT '点击量(次)',
    cpc_platform               float COMMENT '平均点击单价(元/次)',
    ctr                        float COMMENT '点击率(%)',
    convert_cnt                float COMMENT '转化数(个)',
    conversion_rate            float COMMENT '转化率(%)',
    conversion_cost            float COMMENT '转化成本(元/次)',
    game_pay_count             float COMMENT '付费次数(次)',
    game_pay_cost              float COMMENT '付费成本(元/次)',
    customer_effective         float COMMENT '有效获客(次)',
    customer_effective_cost    float COMMENT '有效获客成本(加微成本)(元/次)',
    total_play                 float COMMENT '播放数(次)',
    valid_play                 float COMMENT '有效播放数(次)',
    valid_play_cost            float COMMENT '有效播放成本(元/次)',
    valid_play_rate            float COMMENT '有效播放率(%)',
    play_duration_3s           float COMMENT '3秒播放数(次)',
    play_duration_3s_rate      float COMMENT '3秒播放率(%)',
    play_25_feed_break         float COMMENT '25%进度播放数(次)',
    play_50_feed_break         float COMMENT '50%进度播放数(次)',
    play_75_feed_break         float COMMENT '75%进度播放数(次)',
    play_100_feed_break        float COMMENT '99%进度播放数(次)',
    average_play_time_per_play float COMMENT '平均单次播放时长(s/次)',
    completePlayCount          float COMMENT '播放完成数(次)',
    play_over_rate             float COMMENT '完播率(%)',
    cpm_platform               float COMMENT '平均千次展现费用(元)',
    dy_like                    float COMMENT '点赞数(次)',
    dy_comment                 float COMMENT '评论量(次)',
    dy_share                   float COMMENT '分享量(次)'
)
    COMMENT '培训主题数仓-抖音直播素材_期次_日看板-重新分区'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_dyzbsucai_dashboard_repartition';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.max.created.files=1000000;


INSERT OVERWRITE TABLE app.c_app_course_daily_dyzbsucai_dashboard_repartition PARTITION (dt)
SELECT a.d_date
     , '日' AS group_type
     , ''       AS goods_name
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
     , a.show_cnt
     , a.click_cnt
     , a.cpc_platform
     , a.ctr
     , a.convert_cnt
     , a.conversion_rate
     , a.conversion_cost
     , a.game_pay_count
     , a.game_pay_cost
     , a.customer_effective
     , a.customer_effective_cost
     , a.total_play
     , a.valid_play
     , a.valid_play_cost
     , a.valid_play_rate
     , a.play_duration_3s
     , a.play_duration_3s_rate
     , a.play_25_feed_break
     , a.play_50_feed_break
     , a.play_75_feed_break
     , a.play_100_feed_break
     , a.average_play_time_per_play
     , a.completePlayCount
     , a.play_over_rate
     , a.cpm_platform
     , a.dy_like
     , a.dy_comment
     , a.dy_share
     , a.d_date AS dt
FROM app.c_app_course_daily_dyzbsucai_dashboard a
WHERE a.dt = '${datebuf}'
  AND a.d_date >= '2025-05-20'
  AND a.pos IN ('头条直播付费流')
  AND a.group_type = '1_账户x计划x素材'

UNION ALL

SELECT b.d_date
     , '期次' AS group_type
     , b.period     AS goods_name
     , b.cat
     , b.ad_department
     , b.platform_name
     , b.pos
     , b.price
     , b.link_type_v2
     , b.mobile
     , b.agent
     , b.cost_id
     , b.ad_id
     , b.sucai_id
     , b.sucai_name
     , b.cost
     , b.cost_real
     , b.submit_num
     , b.payment_num
     , b.user_num
     , b.wx_num
     , b.collect_num
     , b.pay_num_d4
     , b.pay_sum_d4
     , b.roi_d4
     , b.pay_user_num
     , b.pay_num
     , b.pay_sum
     , b.roi
     , b.conv_rate
     , b.cac
     , b.wx_rate
     , b.wx_active_rate
     , b.collect_rate
     , b.collect_active_rate
     , b.ifcome0_rate
     , b.ifok0_rate
     , b.ifcome1_rate
     , b.ifok1_rate
     , b.ifcome2_rate
     , b.ifok2_rate
     , b.ifcome3_rate
     , b.ifok3_rate
     , b.ifcome4_rate
     , b.ifok4_rate
     , b.ifcome5_rate
     , b.ifok5_rate
     , b.wx_active_num
     , b.collect_active_num
     , b.ifcome0
     , b.ifok0
     , b.ifcome1
     , b.ifok1
     , b.ifcome2
     , b.ifok2
     , b.ifcome3
     , b.ifok3
     , b.ifcome4
     , b.ifok4
     , b.ifcome5
     , b.ifok5
     , NULL       AS show_cnt
     , NULL       AS click_cnt
     , NULL       AS cpc_platform
     , NULL       AS ctr
     , NULL       AS convert_cnt
     , NULL       AS conversion_rate
     , NULL       AS conversion_cost
     , NULL       AS game_pay_count
     , NULL       AS game_pay_cost
     , NULL       AS customer_effective
     , NULL       AS customer_effective_cost
     , NULL       AS total_play
     , NULL       AS valid_play
     , NULL       AS valid_play_cost
     , NULL       AS valid_play_rate
     , NULL       AS play_duration_3s
     , NULL       AS play_duration_3s_rate
     , NULL       AS play_25_feed_break
     , NULL       AS play_50_feed_break
     , NULL       AS play_75_feed_break
     , NULL       AS play_100_feed_break
     , NULL       AS average_play_time_per_play
     , NULL       AS completePlayCount
     , NULL       AS play_over_rate
     , NULL       AS cpm_platform
     , NULL       AS dy_like
     , NULL       AS dy_comment
     , NULL       AS dy_share
     , b.dt
FROM app.c_app_course_period_dyzbsucai_dashboard b
WHERE b.dt = '${datebuf}'
GROUP BY b.d_date,
        b.period,
        b.cat,
        b.ad_department,
        b.platform_name,
        b.pos,
        b.price,
        b.link_type_v2,
        b.mobile,
        b.agent,
        b.cost_id,
        b.ad_id,
        b.sucai_id,
        b.sucai_name,
        b.cost,
        b.cost_real,
        b.submit_num,
        b.payment_num,
        b.user_num,
        b.wx_num,
        b.collect_num,
        b.pay_num_d4,
        b.pay_sum_d4,
        b.roi_d4,
        b.pay_user_num,
        b.pay_num,
        b.pay_sum,
        b.roi,
        b.conv_rate,
        b.cac,
        b.wx_rate,
        b.wx_active_rate,
        b.collect_rate,
        b.collect_active_rate,
        b.ifcome0_rate,
        b.ifok0_rate,
        b.ifcome1_rate,
        b.ifok1_rate,
        b.ifcome2_rate,
        b.ifok2_rate,
        b.ifcome3_rate,
        b.ifok3_rate,
        b.ifcome4_rate,
        b.ifok4_rate,
        b.ifcome5_rate,
        b.ifok5_rate,
        b.wx_active_num,
        b.collect_active_num,
        b.ifcome0,
        b.ifok0,
        b.ifcome1,
        b.ifok1,
        b.ifcome2,
        b.ifok2,
        b.ifcome3,
        b.ifok3,
        b.ifcome4,
        b.ifok4,
        b.ifcome5,
        b.ifok5,
        b.dt