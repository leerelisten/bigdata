CREATE TABLE IF NOT EXISTS app.c_app_course_daily_live_dashboard_repartition
(
    d_date              string COMMENT '日期',
    grouptype           string COMMENT '分组',
    cat                 string COMMENT '品类',
    ad_department       string COMMENT '投放部门',
    platform_name       string COMMENT '渠道',
    pos                 string COMMENT '版位',
    h5_id               string COMMENT 'H5ID',
    price               string COMMENT '价格',
    link_type_v2        string COMMENT '链路类型(新)',
    mobile              string COMMENT '收集手机号',
    agent               string COMMENT '代理',
    cost_id             string COMMENT '账户id',
    ad_id               string COMMENT '计划id',
    sucai_id            string COMMENT '素材id',
    sucai_name          string COMMENT '素材名称',
    cost                float COMMENT '账面消耗(元)',
    cost_real           float COMMENT '实际消耗(元)',
    submit_num          int COMMENT '表单填写例子数(个)',
    payment_num         int COMMENT '支付成功例子数(个)',
    user_num            int COMMENT '例子数(个)',
    wx_num              int COMMENT '加微例子数(个)',
    pay_num             float COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    roi                 float COMMENT 'ROI=正价课GMV/实际支出',
    cac                 float COMMENT '账面CAC(元/个)',
    cac_real            float COMMENT '实际CAC(元/个)',
    conv_rate           float COMMENT '转化率=正价课订单数/例子数(%)',
    wx_rate             float COMMENT '加微率=加微例子数/例子数(%)',
    wx_active_rate      float COMMENT '主动加微率=主动加微例子数/例子数(%)',
    collect_rate        float COMMENT '问卷率=填问卷例子数/例子数(%)',
    collect_active_rate float COMMENT '主动问卷率=主动填问卷例子数/例子数(%)',
    d0_come_rate        float COMMENT '导学课到课率',
    d0_ok_rate          float COMMENT '导学课完课率',
    d1_come_rate        float COMMENT 'D1到课率',
    d1_ok_rate          float COMMENT 'D1完课率',
    d2_come_rate        float COMMENT 'D2到课率',
    d2_ok_rate          float COMMENT 'D2完课率',
    d3_come_rate        float COMMENT 'D3到课率',
    d3_ok_rate          float COMMENT 'D3完课率',
    d4_come_rate        float COMMENT 'D4到课率',
    d4_ok_rate          float COMMENT 'D4完课率',
    d5_come_rate        float COMMENT 'D5到课率',
    d5_ok_rate          float COMMENT 'D5完课率',
    m_olduv_rate        float COMMENT '重复例子占比(%)',
    m_olduv_rate30      float COMMENT '重复例子占比(30天)(%)'
)
    COMMENT '培训主题数仓-直播日报表-重新分区'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_live_dashboard_repartition';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.max.created.files=1000000;


INSERT OVERWRITE TABLE app.c_app_course_daily_live_dashboard_repartition PARTITION (dt)
SELECT d_date
     , grouptype
     , cat
     , ad_department
     , platform_name
     , pos
     , h5_id
     , price
     , link_type_v2
     , mobile
     , agent
     , cost_id
     , ad_id
     , sucai_id
     , sucai_name
     , cost
     , cost_real
     , submit_num
     , payment_num
     , user_num
     , wx_num
     , pay_num
     , pay_sum
     , roi
     , cac
     , cac_real
     , conv_rate
     , wx_rate
     , wx_active_rate
     , collect_rate
     , collect_active_rate
     , d0_come_rate
     , d0_ok_rate
     , d1_come_rate
     , d1_ok_rate
     , d2_come_rate
     , d2_ok_rate
     , d3_come_rate
     , d3_ok_rate
     , d4_come_rate
     , d4_ok_rate
     , d5_come_rate
     , d5_ok_rate
     , m_olduv_rate
     , m_olduv_rate30
     , d_date AS dt
FROM app.c_app_course_daily_live_dashboard
WHERE dt = '${datebuf}'
  AND d_date >= '2024-05-01'
