CREATE TABLE IF NOT EXISTS dim.dim_place_h5
(
    `id`                bigint,
    `h5_id`             bigint,
    `title`             STRING COMMENT '标题',
    `special_id`        STRING COMMENT '专栏ID',
    `bussiness_line`    STRING COMMENT 'appkphtrc9e6132 培训-舞蹈 appgxppmvex5184 培训-钢琴 2会员',
    `main_pics`         string COMMENT '主体图',
    `bottom_pic`        string COMMENT '底部图',
    `hold_pic`          string COMMENT '挽留图片',
    `price`             bigint COMMENT '价格(分)',
    `sales`             STRING COMMENT '销售二维码数组',
    `sales_group`       int COMMENT '销售推荐池',
    `isdel`             int COMMENT '是否删除',
    `begin_time`        timestamp COMMENT '开课时间',
    `wxapp`             STRING COMMENT '小程序名称',
    `form_title`        string COMMENT '表单头文案',
    `form_pic`          string COMMENT '表单顶部图片',
    `redeem`            string COMMENT '挽回配置',
    `pay_notify_action` STRING COMMENT '支付回调方法',
    `hide_confirm`      int COMMENT '隐藏',
    `created_at`        timestamp,
    `updated_at`        timestamp,
    `age_show`          int COMMENT '年龄是否显示 1 显示 0 不显示',
    `mobile_show`       int COMMENT '是否收集手机号',
    `gender_show`       int COMMENT '是否显示性别',
    `sales_gender`      string COMMENT '销售分性别',
    `platform`          string COMMENT '渠道',
    `remark`            STRING COMMENT '备注',
    `link_type`         int COMMENT '链路类型 1 小程序加微 2获客肋手',
    `countdown_second`  int COMMENT '倒计时秒',
    `config_id`         string COMMENT '企业微信活码配置ID',
    `platform_section`  string COMMENT '版位',
    `category`          string COMMENT '品类',
    `corpid`            string COMMENT '企业微信id',
    `creator_id`        bigint COMMENT '创建人',
    `updator_id`        bigint COMMENT '更新人',
    `report_type`       int COMMENT '1现在逻辑 2同一用户只回传1次B版，3不回传',
    `ext_form_config`   STRING COMMENT '表单额外信息配置',
    `is_refund`         int COMMENT '是否退款 0 不退款 1 退款',
    `plan_id`           bigint COMMENT '投放计划ID',
    `wxshop_id`         STRING COMMENT '视频号小店ID',
    `wxshop_goods_id`   STRING COMMENT '视频号小店商品ID',
    `pop_jiav`          int COMMENT '是否自动弹窗 1 自动弹窗 2 不自动弹窗',
    `advertiser_id`     string COMMENT '广告id',
    `instance_id`       string COMMENT '橙子建站获客助手组件id',
    `special_sale_data` STRING COMMENT '落地页多课程，课程下多接量池配置信息',
    `period_id`         bigint COMMENT 'place_period的id 期次表id',
    `area`              STRING COMMENT '地区：武汉、广州',
    `email_show`        int COMMENT '是否收集email',
    `ad_department`     string COMMENT '投放部门'
) COMMENT '历史H5维表'
    STORED AS ORC;


INSERT OVERWRITE TABLE dim.dim_place_h5
SELECT a.id
     , h5_id
     , title
     , IF(special_id = '', NULL, special_id)                                             AS special_id
     , bussiness_line
     , main_pics
     , IF(bottom_pic = '', NULL, bottom_pic)                                             AS bottom_pic
     , IF(hold_pic = '', NULL, hold_pic)                                                 AS hold_pic
     , price
     , IF(sales = '', NULL, sales)                                                       AS sales
     , sales_group
     , isdel
     , begin_time
     , wxapp
     , IF(form_title = '', NULL, form_title)                                             AS form_title
     , IF(form_pic = '', NULL, form_pic)                                                 AS form_pic
     , IF(redeem = '', NULL, redeem)                                                     AS redeem
     , IF(pay_notify_action = '', NULL, pay_notify_action)                               AS pay_notify_action
     , hide_confirm
     , created_at
     , updated_at
     , age_show
     , mobile_show
     , gender_show
     , IF(sales_gender = '', NULL, sales_gender)                                         AS sales_gender
     , IF(platform = '', NULL, platform)                                                 AS platform
     , IF(remark = '' OR remark = 'null', NULL, remark)                                  AS remark
     , link_type
     , countdown_second
     , IF(config_id = '', NULL, config_id)                                               AS config_id
     , IF(platform_section = '', NULL, platform_section)                                 AS platform_section
     , IF(category = '', NULL, category)                                                 AS category
     , IF(corpid = '', NULL, corpid)                                                     AS corpid
     , creator_id
     , updator_id
     , report_type
     , IF(ext_form_config = '' OR ext_form_config = 'null', NULL, ext_form_config)       AS ext_form_config
     , is_refund
     , plan_id
     , IF(wxshop_id = '' OR wxshop_id = 'null', NULL, wxshop_id)                         AS wxshop_id
     , IF(wxshop_goods_id = '' OR wxshop_goods_id = 'null', NULL, wxshop_goods_id)       AS wxshop_goods_id
     , pop_jiav
     , IF(advertiser_id = '' OR advertiser_id = 'null', NULL, advertiser_id)             AS advertiser_id
     , IF(instance_id = '' OR instance_id = 'null', NULL, instance_id)                   AS instance_id
     , IF(special_sale_data = '' OR special_sale_data = 'null', NULL, special_sale_data) AS special_sale_data
     , period_id
     , area
     , email_show
     , IF(b.name = '' OR b.name = 'null', NULL, b.name)                                  AS ad_department
FROM ods.ods_dim_place_h5 a
         LEFT JOIN
     (SELECT id, name
      FROM ods.ods_crm_department_group
      WHERE parentid = 700) b
     ON a.ad_department = b.id