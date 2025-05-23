set mapred.job.name="c_app_course_daily_ad_dashboard_permission#${datebuf}";
use app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_daily_ad_dashboard_permission
(
    d_date                  string  comment '日期',
    grouptype               string  comment '分组类型',
    cat                     string  comment '品类',
    platform_name           string  comment '渠道',
    pos                     string  comment '版位',
    price                   string  comment '价格',
    link_type_v2            string  comment '链路类型(新)',
    mobile                  string  comment '收集手机号',
    agent                   string  comment '代理',
    cost_id                 string  comment '账户id',
    ad_id                   string  comment '计划id',
    cost                    float   comment '账面消耗(元)',
    cost_real               float   comment '实际消耗(元)',
    submit_num              int     comment '表单填写例子数(个)',
    payment_num             int     comment '支付成功例子数(个)',
    user_num                int     comment '例子数(个)',
    wx_num                  int     comment '加微例子数(个)',
    collect_num             int     comment '填问卷例子数(个)',
    pay_user_num            int     comment '购买正价课例子数(个)',
    pay_num                 float   comment '正价课订单数(个)',
    pay_sum                 float   comment '正价课GMV(元)',
    roi                     float   comment 'ROI',
    conv_rate               float   comment '转化率(%)',
    cac                     float   comment 'CAC(元/个)',
    wx_rate                 float   comment '加微率(%)',
    wx_active_rate          float   comment '主动加微率(%)',
    collect_rate            float   comment '问卷率(%)',
    collect_active_rate     float   comment '主动问卷率(%)',
    ifcome0_rate            float   comment '导学课到课率(%)',
    ifok0_rate              float   comment '导学课完课率(%)',
    ifcome1_rate            float   comment 'D1到课率(%)',
    ifok1_rate              float   comment 'D1完课率(%)',
    ifcome2_rate            float   comment 'D2到课率(%)',
    ifok2_rate              float   comment 'D2完课率(%)',
    ifcome3_rate            float   comment 'D3到课率(%)',
    ifok3_rate              float   comment 'D3完课率(%)',
    ifcome4_rate            float   comment 'D4到课率(%)',
    ifok4_rate              float   comment 'D4完课率(%)',
    ifcome5_rate            float   comment 'D5到课率(%)',
    ifok5_rate              float   comment 'D5完课率(%)',
    wx_active_num           int     comment '主动加微例子数(个)',
    collect_active_num      int     comment '主动填问卷例子数(个)',
    ifcome0                 int     comment '导学课到课例子数(个)',
    ifok0                   int     comment '导学课完课例子数(个)',
    ifcome1                 int     comment 'D1到课例子数(个)',
    ifok1                   int     comment 'D1完课例子数(个)',
    ifcome2                 int     comment 'D2到课例子数(个)',
    ifok2                   int     comment 'D2完课例子数(个)',
    ifcome3                 int     comment 'D3到课例子数(个)',
    ifok3                   int     comment 'D3完课例子数(个)',
    ifcome4                 int     comment 'D4到课例子数(个)',
    ifok4                   int     comment 'D4完课例子数(个)',
    ifcome5                 int     comment 'D5到课例子数(个)',
    ifok5                   int     comment 'D5完课例子数(个)',
    permission              string  comment '权限用户'
)
    COMMENT '培训主题数仓-投放数据日报表-权限测试'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_ad_dashboard_permission';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.max.created.files=1000000;


INSERT OVERWRITE TABLE app.c_app_course_daily_ad_dashboard_permission partition (dt)
SELECT  a.d_date
       ,a.grouptype
       ,a.cat
       ,a.platform_name
       ,a.pos
       ,a.price
       ,a.link_type_v2
       ,a.mobile
       ,a.agent
       ,a.cost_id
       ,a.ad_id
       ,a.cost
       ,a.cost_real
       ,a.submit_num
       ,a.payment_num
       ,a.user_num
       ,a.wx_num
       ,a.collect_num
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
       ,concat('zhangy@tangdou.com,huangr@tangdou.com,zhangd@tangdou.com,zhout@tangdou.com,yinky@tangdou.com,wangdonglei@tangdou.com ,zhengm@tangdou.com,tangwenqi@tangdou.com,wanyihan@tangdou.com,wangying1@tangdou.com,lihuaqi@tangdou.com,huangchao@tangdou.com,zhangxl@tangdou.com,qinlj@tangdou.com,kangbin@tangdou.com,shuchang@tangdou.com',
            case when platform_name = '抖音' and pos = '头条直播付费流' then 'yangyang@tangdou.com,chenfeng@tangdou.com,zhangwenwen@tangdou.com,wuchong@tangdou.com,guoxuxin@tangdou.com,wangxuebing@tangdou.com,huangyanjun@tangdou.com,mayuanfei@tangdou.com,shipuyu@tangdou.com,chenghr@tangdou.com'
                 when platform_name = '抖音' and pos = '头条直播免费流' then 'yangyang@tangdou.com,chenfeng@tangdou.com,zhangwenwen@tangdou.com,mayuanfei@tangdou.com,shipuyu@tangdou.com,chenghr@tangdou.com'
                 when platform_name = '抖音' and pos = '头条信息流' then 'yangwenwen@tangdou.com,tongjingxi@tangdou.com,gannanxiao@tangdou.com,wangqi@tangdou.com,lihaixu@tangdou.com,caizhenglin@tangdou.com'
                 when platform_name = '抖音' and pos = '千川直播' then 'mawenfei@tangdou.com'
                 when platform_name = '腾讯' and pos = '腾讯视频号付费流' then 'zhanghuijing@tangdou.com'
                 when platform_name = '腾讯' and pos = '腾讯视频号免费流' then 'zhanghuijing@tangdou.com'
                 when platform_name in ('腾讯','腾讯pcad') and pos in ('腾讯公众号','腾讯朋友圈','腾讯视频号信息流','腾讯pcad','腾讯视频号')
                            then 'yangwenwen@tangdou.com,wangzy@tangdou.com,tongjingxi@tangdou.com,huqinglan@tangdou.com,miaoqishen@tangdou.com,gaocan@tangdou.com,xiaoao@tangdou.com,wangyutian@tangdou.com,caizhenglin@tangdou.com'
                 when platform_name = '腾讯公众号关注' and pos is not null then 'yangwenwen@tangdou.com,wangzy@tangdou.com,tongjingxi@tangdou.com,huqinglan@tangdou.com,miaoqishen@tangdou.com,gaocan@tangdou.com,xiaoao@tangdou.com,caizhenglin@tangdou.com'
                 when platform_name = '百度' and pos in ('百度搜索','百度信息流') then 'yangwenwen@tangdou.com,wangzy@tangdou.com,tongjingxi@tangdou.com,huqinglan@tangdou.com,xiaoao@tangdou.com,yuanjunjun@tangdou.com,wangyutian@tangdou.com,caizhenglin@tangdou.com'
                 when platform_name = '快手' and pos is not null then 'yangwenwen@tangdou.com,wangzy@tangdou.com,tongjingxi@tangdou.com,huqinglan@tangdou.com,caizhenglin@tangdou.com'
                 when platform_name = '快手' and pos is not null then 'yangwenwen@tangdou.com,wangzy@tangdou.com,tongjingxi@tangdou.com,huqinglan@tangdou.com,caizhenglin@tangdou.com'
                 when platform_name in ('非标','SDK聚合') then 'yangwenwen@tangdou.com,panyijie@tangdou.com'
                 else ''
            end
            ) as permission
       ,a.d_date AS dt
FROM app.c_app_course_daily_ad_dashboard a
WHERE a.dt = '${datebuf}'
and a.d_date >= '2024-05-01'
;