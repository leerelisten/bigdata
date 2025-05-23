SET mapred.job.name="da_course_daily_cost_by_adid#${datebuf}";
USE da;
CREATE TABLE IF NOT EXISTS da.da_course_daily_cost_by_adid
(
    d_date          string COMMENT '投放日期',
    cat             string COMMENT '品类',
    platform        string COMMENT '渠道',
    price           string COMMENT '价格',
    mobile          string COMMENT '是否收集手机号',
    link_type_v1    string COMMENT '链路类型(旧)',
    link_type_v2    string COMMENT '链路类型(新)',
    pos             string COMMENT '版位',
    ad_department   string COMMENT '投放部门',
    name            string COMMENT '代理名称',
    advertiser_name string COMMENT '账户名称',
    cost_id         string COMMENT '账户id',
    agent           string COMMENT '代理',
    ad_id           string COMMENT '计划id',
    type            string COMMENT '投放类型',
    cost            float COMMENT '账面消耗',
    cost_real       float COMMENT '实际消耗',
    show_cnt        int COMMENT '曝光量pv',
    click_cnt       int COMMENT '点击量pv',
    pay_cnt         int COMMENT '付费(支付1元)量pv',
    convert_cnt     int COMMENT '转化(加微)量pv',
    country_code    string COMMENT '国家编码'
)
    COMMENT '培训主题数仓-投放属性-计划id粒度消耗数据'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/da/da_course_daily_cost_by_adid';


SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;
SET hive.execution.engine=tez;


WITH base AS
         (SELECT t1.cdate                      AS dt
               , CASE
                     WHEN t1.advertiser_id RLIKE '直播.组' THEN '太极'
                     ELSE t2.d_cat END         AS cat
               , CASE
                     WHEN t1.advertiser_id RLIKE '直播一组' THEN '抖音'
                 -- 25.4.8日新增，复投千川直播
                     WHEN t1.advertiser_id = '直播二组-千川直播' THEN '抖音'
                     WHEN t1.advertiser_id RLIKE '直播二组' THEN '腾讯'
                     WHEN t1.advertiser_id RLIKE '直播三组' THEN '抖音'
                     ELSE t2.d_platform END    AS platform
               , t2.price                      AS price
               , t2.collect_phone_type         AS mobile
               , t2.link_type_v1
               , t2.link_type_v2
               , CASE
                     WHEN t1.advertiser_id = '直播二组-千川直播' THEN '千川直播'
                     WHEN t1.advertiser_id = '直播一组-本地推直播' THEN '本地推直播'
                 -- 25.3.28 ,添加直播二组9.9元测试
                     WHEN t1.advertiser_id = '直播二组-视频号直播测试' THEN '腾讯视频号直播付费流'
                 -- 由于直播二组录错，25.3.24改成精准匹配
                     WHEN t1.advertiser_id = '直播二组-视频号直播' THEN '腾讯视频号直播付费流'
                     WHEN t1.advertiser_id RLIKE '抖加' THEN '抖加'
                     WHEN t1.advertiser_id = '直播一组-千川直播（虚拟）' THEN '千川直播（虚拟）'
                     WHEN t1.advertiser_id = '直播三组-千川直播' THEN '千川直播'
                     WHEN t1.advertiser_id RLIKE '小店随心推' THEN '小店随心推'
                     WHEN t1.advertiser_id = '非标-大屏' THEN '大屏'
                     WHEN t1.advertiser_id = '直播二组-视频号直播（瑜伽）' THEN '腾讯视频号直播付费流（瑜伽）'
                     WHEN t1.advertiser_id = '非标-太极APP（抖音）' THEN '抖音信息流下载'
                     WHEN t1.advertiser_id = '非标-太极APP（腾讯）' THEN '腾讯信息流下载'
                     WHEN t1.advertiser_id = '非标-太极APP（百度）' THEN '百度信息流下载'
                     WHEN t1.advertiser_id = '非标-太极APP（快手）' THEN '快手信息流下载'
                     WHEN t1.advertiser_id = '海外投放组-谷歌（东南亚）' THEN '谷歌（东南亚）'
                     WHEN t1.advertiser_id = '海外投放组-谷歌（北美）' THEN '谷歌（北美）'
                     WHEN t1.advertiser_id = '海外投放组-脸书（东南亚）' THEN '脸书（东南亚）'
                     WHEN t1.advertiser_id = '海外投放组-脸书（北美）' THEN '脸书（北美）'
                     WHEN t1.advertiser_id = '海外投放组-TikTok（东南亚）' THEN '海外投放组-TikTok（东南亚）'
                     WHEN t1.advertiser_id = '海外投放组-TikTok（北美）' THEN '海外投放组-TikTok（北美）'
                     WHEN t1.advertiser_id = '信息流一部-新浪粉丝通' THEN '信息流一部-新浪粉丝通'
                     WHEN t1.advertiser_id = '信息流一部-360网盟' THEN '信息流一部-360网盟'
                     WHEN t1.advertiser_id = '直播二组-视频号直播八段锦' THEN '直播二组-视频号直播八段锦'
                     WHEN t1.advertiser_id = '海外投放组-腾讯东南亚' THEN '海外投放组-腾讯东南亚'
                     WHEN t1.advertiser_id = '直播二组-千川直播八段锦' THEN '直播二组-千川直播八段锦'
                     WHEN t1.advertiser_id = '信息流三部-谷歌（东南亚）下载' THEN '信息流三部-谷歌（东南亚）下载'
                     WHEN t1.advertiser_id = '信息流三部-脸书（东南亚）下载' THEN '信息流三部-脸书（东南亚）下载'
                     ELSE t2.d_pos END         AS pos
               , t2.ad_department
               , t2.open_agent                 AS open_agent
               , t2.operation_agent            AS operation_agent
               , t2.material_agent             AS material_agent
               , CASE
                     WHEN t2.d_pos = '百度信息流北京' AND cdate BETWEEN '2025-03-06' AND '2025-03-12' THEN '百度信息流北京'
                     WHEN t2.d_pos = '腾讯pcad(北京)' AND cdate BETWEEN '2025-03-14' AND '2025-03-17'
                         THEN '腾讯pcad(北京)'
                     WHEN t2.d_pos = '腾讯优量汇(北京)' AND cdate BETWEEN '2025-03-13' AND '2025-03-17'
                         THEN '腾讯优量汇(北京)'
                 -- 2025.5.13  lyf 信息流四部单独处理
                     WHEN t2.d_pos = '抖音信息流下载' AND cdate >= '2025-03-30' AND
                          ad_department = '信息流四部' THEN '信息流四部-太极APP-抖音信息流下载'
                     WHEN t2.d_pos = '抖音信息流下载' AND cdate >= '2025-03-30'
                         THEN '太极APP-抖音信息流下载'
                     WHEN t2.d_pos = '腾讯信息流下载' AND cdate >= '2025-03-30' THEN '太极APP-腾讯信息流下载'
                     WHEN t2.d_pos = '百度信息流下载' THEN '太极APP-百度信息流下载'
                     WHEN t2.d_pos = '百度搜索下载' THEN '太极APP-百度搜索下载' -- 20250425 百度的使用渠道版位硬关联
                     WHEN t2.d_platform = '腾讯公众号关注' AND t2.d_pos = '腾讯朋友圈' AND
                          cdate BETWEEN '2025-04-01' AND '2025-04-03' THEN '腾讯公众号关注-腾讯朋友圈'
                     WHEN
                         t2.d_platform = '太极APP' AND t2.d_pos = 'oppo商店下载' AND cdate >= '2025-04-18'
                         THEN '太极APP-oppo商店下载' --20250422 应用商店下载--> oppo商店下载
                     WHEN
                         t2.d_platform = '太极APP' AND t2.d_pos = 'oppo信息流下载' AND cdate >= '2025-04-22'
                         THEN '太极APP-oppo信息流下载' --20250422 新增
                     WHEN
                         t2.d_platform = '太极APP' AND t2.d_pos = 'vivo商店下载' AND cdate >= '2025-04-22'
                         THEN '太极APP-vivo商店下载' --20250422 新增
                     WHEN
                         t2.d_platform = '太极APP' AND t2.d_pos = 'vivo信息流下载' AND cdate >= '2025-04-22'
                         THEN '太极APP-vivo信息流下载' --20250422 新增
                     WHEN
                         t2.d_platform = '太极APP' AND t2.d_pos = '华为商店下载' AND cdate >= '2025-04-22'
                         THEN '太极APP-华为商店下载' --20250422 新增
                     WHEN
                         t2.d_platform = '太极APP' AND t2.d_pos = '华为ads下载' AND cdate >= '2025-04-22'
                         THEN '太极APP-华为ads下载' --20250422 新增
                     WHEN t2.d_platform = '谷歌' AND cdate >= '2025-04-19'
                         THEN CONCAT(t1.advertiser_id, '&', t3.country_code)
                     ELSE t1.advertiser_id END AS costid
               , t1.promotion_id               AS adid
               , t1.cost
               , t1.show_cnt                   AS dv
               , t1.click_cnt                  AS ck
               , t2.ad_method                  AS type
               , t2.full_version_name          AS advertiser_name
               , pay_count                     AS pay_count
               , convert_cnt                   AS convert_cnt
               , CASE
                     WHEN cdate >= '2024-10-12'
                         AND cost_id IN (
                                         '1799179576207372', '1803724956425216', '1803724878692378',
                                         '1803724959689737', '1824381426312282'
                                             '1794472206435465'
                             )
                         THEN '二类'
                     WHEN cdate >= '2024-11-26'
                         AND cost_id = '1803725157131419'
                         THEN '二类'
                     WHEN cdate >= '2024-12-21'
                         AND cost_id = '1810685219322185'
                         THEN '二类'
                     WHEN cdate >= '2024-12-25'
                         AND cost_id = '1810685220529177'
                         THEN '二类'
                     WHEN cdate >= '2024-12-31'
                         AND cost_id IN ('1814944431398107', '1814944430876682')
                         THEN '二类'
                     WHEN cdate >= '2025-01-07'
                         AND cost_id = '1795114277996554'
                         THEN '二类'
                     WHEN cdate >= '2025-01-09'
                         AND cost_id = '1806697066208361'
                         THEN '二类'
                     WHEN cdate >= '2025-02-05'
                         AND cost_id = '1797548050126858'
                         THEN '二类'
                     WHEN cdate >= '2025-02-10' -- 在线表格是2月15号,不做修改
                         AND cost_id = '1814944335535241'
                         THEN '二类'
                     WHEN cdate >= '2025-02-15'
                         AND cost_id IN ('1797548049496076', '1797548050773066', '1797548053759081')
                         THEN '二类'
                     WHEN cdate >= '2025-02-27'
                         AND cost_id IN ('1814944330692953', '1814944331807747')
                         THEN '二类'
                     WHEN cost_id IN ('1814944332971019', '1814944332404761', '1814944432991243')
                         THEN '二类'
                     WHEN cdate >= '2025-04-07'
                         AND cost_id = '1803724961072330'
                         THEN '二类'
                 -- 25.4.7 樊启斌确认，抖音信息流北京全部为二类账号
                     WHEN cdate >= '2025-04-07'
                         AND d_pos = '头条信息流(北京)'
                         THEN '二类'
                     WHEN cdate >= '2025-04-23'
                         AND
                          cost_id IN ('1824381427477594', '1824381430018059', '1824381428546041', '1824381428014276',
                                      '1824381431284235', '1826537542568395')
                         THEN '二类'
                     WHEN cdate >= '2025-04-25'
                         AND
                          cost_id IN ('1826537537658203', '1826537649489088', '1826537451517964', '1826537452193099',
                                      '1826537452777483')
                         THEN '二类'
                     WHEN cdate >= '2025-04-28'
                         AND cost_id = '1803725152880651'
                         THEN '二类'
                     ELSE '其他'
                 END                           AS douyin_type
               , '自运营'                      AS douyin_flag
               , t3.country_code
          FROM (SELECT cdate                      --抓取日期
                     , platform                   --渠道
                     , advertiser_id              --账户id
                     , promotion_id               --计划id
                     , cost * rate_to_cny AS cost --账户消耗
                     , show_cnt                   --曝光pv
                     , click_cnt                  --点击pv
                     , pay_count                  --付费(支付1元)pv
                     , convert_cnt                --转化(加微)pv
                     , rate_to_cny
                     , country_id
                FROM ods.ods_marketing_report --api抓取的原始数据
                WHERE platform != 10
                   -- 25.03.24 修改手动录入消耗的范围，避免消耗录入错误
                   OR (platform = 10 AND advertiser_id IN (
                                                           '直播二组-视频号直播测试',
                                                           '直播二组-视频号直播',
                                                           '直播三组-千川直播',
                                                           '直播二组-千川直播',
                                                           '直播一组-千川直播（虚拟）',
                                                           '非标-太极APP（抖音）',
                                                           '非标-太极APP（百度）',
                                                           '非标-大屏',
                                                           '直播一组-本地推直播',
                                                           '海外投放组-脸书（东南亚）',
                                                           '海外投放组-脸书（北美）',
                                                           '海外投放组-谷歌（北美）',
                                                           '海外投放组-谷歌（东南亚）',
                                                           '非标-太极APP（腾讯）',
                                                           '直播三组-直播三组',
                                                           '直播二组-视频号直播（瑜伽）',
                                                           '非标-太极APP（快手）',
                                                           '海外投放组-TikTok（东南亚）',
                                                           '海外投放组-TikTok（北美）',
                                                           '信息流一部-360网盟',
                                                           '信息流一部-新浪粉丝通',
                                                           '直播二组-视频号直播八段锦',
                                                           '直播二组-千川直播八段锦',
                                                           '信息流三部-谷歌（东南亚）下载',
                                                           '信息流三部-脸书（东南亚）下载',
                                                           '直播一组-千川直播（虚拟）八段锦'
                    ))
                   -- WHERE cdate <= '${datebuf}' --20240821改为单dt内记录所有数据 -- 兼容导入数据
               ) t1
                   LEFT JOIN
               (SELECT ad_department                                                       -- 25.03.11 新增投放部门
                     , cost_id                                                             -- 账户id
                     , d_cat                                                               -- 品类
                     , d_platform                                                          -- 渠道
                     , price                                                               -- 价格
                     , collect_phone_type                                                  -- 手机手机号类型
                     , CONCAT(NVL(price, ''), NVL(collect_phone_type, '')) AS link_type_v1 -- 链路类型(旧)
                     , d_linktype                                          AS link_type_v2
                     , d_pos                                                               -- 版位
                     , open_agent                                                          -- 开户代理
                     , operation_agent                                                     -- 运营代理
                     , material_agent                                                      -- 素材代理
                     , ad_method                                                           -- 自投/代投
                     , full_version_name                                                   -- 完整名称
                FROM (SELECT *
                           , ROW_NUMBER() OVER (PARTITION BY cost_id ORDER BY updated_at DESC) AS rnum
                      FROM dim.dim_place_costname) tmp
                WHERE rnum = 1) t2
               ON t1.advertiser_id = t2.cost_id
                   LEFT JOIN ods.ods_geotargets t3
                             ON t1.country_id = t3.criteria_id
          -- 24.11.23  过滤ADQ账户，避免影响日报
          WHERE t1.advertiser_id NOT IN (
              --24.12.03 添加 51842463 51842455  51829940 51829945 51829951 51829957 51829964
              -- 24.11.23添加46500701
              -- 25.3.18 添加57563973 57563977 57563982 57563985 57563991
                                         '51240623',
                                         '51240618',
                                         '49187018',
                                         '49187023',
                                         '49187033',
                                         '49187044',
                                         '49187048',
                                         '49187055',
                                         '49187063',
                                         '58028438',
                                         '58028429',
                                         '58028422',
                                         '56517598',
                                         '56517590',
                                         '58028413',
                                         '58028405',
                                         '57563973',
                                         '57563977',
                                         '57563982',
                                         '57563985',
                                         '57563991',
                                         '46500712',
                                         '46500718',
                                         '46500701',
                                         '51842463',
                                         '51842455',
                                         '49203184',
                                         '50841483',
                                         '50841487',
                                         '51829940',
                                         '51829945',
                                         '51829951',
                                         '51829957',
                                         '51829964',
                                         '49187070',
                                         '49187077',
                                         '49187087'
              ))
--20250218 安晓康 调整 消耗类型 太极APP--》太极APP（抖音） 新增 太极APP（腾讯）
--背景 20250113 新增手工录入数据 太极APP 其对应3个版位 需要按照例子占比均摊
--    , user_info AS (
--     SELECT cost_id2
--          , d_date
--          , pos
--          , cnt
--          , all_cnt
--          , (NVL(cnt, 0) / all_cnt) AS coefficien
--     FROM (
--              SELECT cost_id2
--                   , d_date
--                   , pos
--                   , COUNT(member_id) OVER (PARTITION BY pos,d_date) AS cnt     --每个期次对应版位例子
--                   , COUNT(member_id) OVER (PARTITION BY d_date)     AS all_cnt --每个期次对应例子
--              FROM dws.dws_sale_camping_user_day
--              WHERE dt = '${datebuf}'
--                AND member_status = 1
--                AND trade_state IN ('SUCCESS', 'PREPARE')
--                AND sales_id > 0
--                AND pos IN ('抖音信息流下载', '腾讯信息流下载', '快手信息流下载') --只取太极APP对应的版位
--          ) a
--     GROUP BY cost_id2, d_date, pos, cnt, all_cnt
-- )
--    , base_data2 AS (
--     --关联例子获取版位和对应的消耗占比
--     SELECT dt,
--            cat,
--            platform,
--            price,
--            mobile,
--            link_type_v1,
--            link_type_v2,
--            CASE WHEN b.cost_id2 IS NOT NULL AND b.d_date IS NOT NULL THEN b.pos ELSE a.pos END                  AS pos,
--            open_agent,
--            operation_agent,
--            material_agent,
--            costid,
--            adid,
--            CASE WHEN b.cost_id2 IS NOT NULL AND b.d_date IS NOT NULL THEN a.cost * b.coefficien ELSE a.cost END AS cost,
--            dv,
--            ck,
--            type,
--            advertiser_name,
--            pay_count,
--            convert_cnt,
--            douyin_type
--     FROM base a
--              LEFT JOIN user_info b
--                        ON a.costid = b.cost_id2
--                            AND a.dt = b.d_date
-- )


INSERT
OVERWRITE
TABLE
da.da_course_daily_cost_by_adid
PARTITION
(
dt = '${datebuf}'
)
SELECT dt
     , cat
     , CASE
           WHEN platform = '腾讯pcad' THEN '腾讯'
           ELSE IF(platform = '' OR platform IS NULL, '其他', platform) END      AS platform
     , price
     , CASE
           WHEN mobile IN ('收集手机号', '收号', '有号', '授号', '小程序授权手机号') THEN '有手机号'
           WHEN mobile IN ('无', '无号', '无手机号') THEN '无手机号'
           ELSE '无手机号' END                                                   AS mobile
     , link_type_v1
     , CASE
           WHEN link_type_v2 = '小程序' THEN '小程序加微'
           WHEN link_type_v2 = '新一键' THEN '新一键授权'
           WHEN link_type_v2 = '获客' THEN '获客助手'
           ELSE link_type_v2 END                                                 AS link_type_v2
     , IF(pos = '' OR pos IS NULL, '其他', pos)                                  AS pos
     , ad_department
     , operation_agent
     , advertiser_name
     , costid                                                                    AS cost_id
     , CONCAT_WS('-', SPLIT(advertiser_name, '-')[1], SPLIT(advertiser_name, '-')[2],
                 SPLIT(advertiser_name, '-')[3], SPLIT(advertiser_name, '-')[4]) AS agent
     , adid                                                                      AS ad_id
     , type
     , cost
     , CASE
           WHEN dt >= '2025-01-01' THEN
               CASE
                   WHEN platform = '抖音' AND pos = '千川直播' AND open_agent = '创研'
                       THEN NVL(cost, 0) / (1 + 0.00)
                   WHEN platform = '抖音' AND pos = '千川直播' AND open_agent = '速及'
                       THEN NVL(cost, 0) / (1 + 0.01)
                   -- 未接入消耗
                   --    when platform = '抖音' and pos = '本地推' and open_agent rlike '创研' and type = '自投'
                   --        then nvl(cost, 0) / (1 + 0.02)
                   WHEN platform = '抖音' AND open_agent = '创研' AND douyin_type = '二类' AND type = '自投'
                       THEN CASE
                                WHEN douyin_flag = '自运营' THEN NVL(cost, 0) / (1 + 0.04)
                                ELSE NVL(cost, 0) / (1 + 0.00) END

                   WHEN platform = '抖音' AND open_agent = '创研' AND douyin_type = '二类' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.02)

                   WHEN platform = '抖音' AND open_agent = '创研' AND douyin_type <> '二类' AND type = '自投'
                       THEN CASE
                                WHEN material_agent = '创研' THEN NVL(cost, 0) / (1 + 0.015)
                                WHEN douyin_flag = '自运营' THEN NVL(cost, 0) / (1 + 0.025)
                                ELSE NVL(cost, 0) / (1 + 0.00) END

                   WHEN platform = '抖音' AND open_agent = '创研' AND douyin_type <> '二类' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.00)

                   WHEN platform = '抖音' AND open_agent = '速及' AND douyin_type = '二类' AND type = '自投'
                       THEN CASE
                                WHEN douyin_flag = '自运营' THEN NVL(cost, 0) / (1 + 0.065)
                                WHEN douyin_flag = '走量' THEN NVL(cost, 0) / (1 + 0.03)
                                ELSE NVL(cost, 0) / (1 + 0.00) END
                   WHEN platform = '抖音' AND open_agent = '速及' AND douyin_type = '四类' AND type = '自投'
                       THEN CASE
                                WHEN douyin_flag = '自运营' THEN NVL(cost, 0) / (1 + 0.04)
                                WHEN douyin_flag = '走量' THEN NVL(cost, 0) / (1 + 0.02)
                                ELSE NVL(cost, 0) / (1 + 0.00) END

                   WHEN platform = '抖音' AND open_agent = '速及' AND douyin_type NOT IN ('二类', '四类') AND
                        type = '自投'
                       THEN CASE
                                WHEN douyin_flag = '自运营' THEN NVL(cost, 0) / (1 + 0.035)
                                WHEN douyin_flag = '走量' THEN NVL(cost, 0) / (1 + 0.02)
                                ELSE NVL(cost, 0) / (1 + 0.00) END

                   WHEN platform = '抖音' AND open_agent = '深圳厚拓' AND douyin_type = '二类' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.03)
                   WHEN platform = '抖音' AND open_agent = '深圳厚拓' AND douyin_type <> '二类' AND type = '代投'
                       THEN NVL(cost, 0) / (1 - 0.01)

                   -- 25.3.31新增，和肖傲沟通，新增腾讯 运营代理为引领时，无返点。
                   WHEN platform IN ('腾讯', '腾讯pcad', '腾讯公众号关注', '腾讯优量汇') AND
                        open_agent IN ('微盟', '北京微盟') AND
                        operation_agent = '引领'
                       THEN NVL(cost, 0)
                   -- 整体调整腾讯渠道的返点 所有腾讯相关的渠道均按照4%/1%的比例抵扣
                   WHEN platform IN ('腾讯', '腾讯pcad', '腾讯公众号关注', '腾讯优量汇') AND
                        open_agent IN ('微盟', '北京微盟') AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.04)
                   WHEN platform IN ('腾讯', '腾讯pcad', '腾讯公众号关注', '腾讯优量汇') AND
                        open_agent IN ('微盟', '北京微盟') AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.01)
                   WHEN platform IN ('腾讯', '腾讯pcad', '腾讯公众号关注', '腾讯优量汇') AND open_agent = '京雅' AND
                        material_agent = '京雅' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.01)
                   WHEN platform IN ('腾讯', '腾讯pcad', '腾讯公众号关注', '腾讯优量汇') AND open_agent = '京雅' AND
                        material_agent = '京雅' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.00)
                   WHEN platform IN ('腾讯', '腾讯pcad', '腾讯公众号关注', '腾讯优量汇') AND open_agent = '京雅' AND
                        material_agent <> '京雅' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.04)
                   WHEN platform IN ('腾讯', '腾讯pcad', '腾讯公众号关注', '腾讯优量汇') AND open_agent = '京雅' AND
                        material_agent <> '京雅' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.01)


                   WHEN pos = '百度直播付费流'
                       THEN NVL(cost, 0) / (1 + 0.59)

                   -- 20250403百度启用新返点规则
                   WHEN platform = '百度' AND dt >= '2025-04-03'
                       THEN CASE
                                WHEN platform = '百度' AND type = '代投' AND pos = '百度搜索'
                                    THEN CASE
                                             WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.30)
                                             WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.301)
                                             WHEN open_agent = '赢宝' THEN NVL(cost, 0) / (1 + 0.30) -- 去掉日期限制，回刷五月以来的数据
                                             ELSE NVL(cost, 0) END
                                WHEN platform = '百度' AND type = '自投' AND pos = '百度搜索'
                                    THEN CASE
                                             WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.35)
                                             WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.351)
                                             ELSE NVL(cost, 0) END
                                WHEN platform = '百度' AND type = '代投' AND pos IN ('百度信息流', '百度信息流北京')
                                    THEN CASE
                                             WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.33)
                                             WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.331)
                                             WHEN open_agent = '赢宝' THEN NVL(cost, 0) / (1 + 0.48) -- 去掉日期限制，回刷五月以来的数据
                                             ELSE NVL(cost, 0) END
                                WHEN platform = '百度' AND type = '自投' AND pos IN ('百度信息流', '百度信息流北京')
                                    THEN CASE
                                             WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.38)
                                             WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.381)
                                             ELSE NVL(cost, 0) END
                                ELSE NVL(cost, 0) END

                   WHEN platform = '百度' AND dt <= '2025-04-02'
                       THEN CASE
                                WHEN type = '代投' AND pos = '百度搜索'
                                    THEN NVL(cost, 0) / (1 + 0.26)
                                WHEN type = '自投' AND pos = '百度搜索'
                                    THEN NVL(cost, 0) / (1 + 0.35)
                                WHEN type = '代投' AND pos IN ('百度信息流', '百度信息流北京')
                                    THEN NVL(cost, 0) / (1 + 0.32)
                                WHEN type = '自投' AND pos IN ('百度信息流', '百度信息流北京')
                                    THEN NVL(cost, 0) / (1 + 0.41)
                       END

                   WHEN platform = '太极APP'
                       THEN CASE
                                WHEN pos = '抖音信息流下载' THEN
                                    CASE
                                        WHEN type = '代投' THEN
                                            CASE
                                                WHEN open_agent = '广知' AND dt >= '2025-03-01' THEN NVL(cost, 0)
                                                WHEN open_agent = '广知' AND dt < '2025-03-01'
                                                    THEN NVL(cost, 0) / (1 + 0.01)
                                                WHEN open_agent IN ('云流量', '引领') THEN NVL(cost, 0)
                                                WHEN open_agent = '经纬' THEN NVL(cost, 0) * 1.03
                                                WHEN open_agent = '聚拓' THEN NVL(cost, 0) * 1.04
                                                ELSE NVL(cost, 0)
                                                END
                                        WHEN type = '自投' THEN
                                            CASE
                                                WHEN douyin_flag = '自运营' THEN NVL(cost, 0) / (1 + 0.04)
                                                WHEN douyin_flag = '走量' THEN NVL(cost, 0) / (1 + 0.02)
                                                ELSE NVL(cost, 0) / (1 + 0.00)
                                                END
                                        END
                                WHEN pos = '腾讯信息流下载' THEN
                                    CASE
                                        WHEN open_agent = '微盟' THEN NVL(cost, 0) / (1 + 0.01)
                                        WHEN open_agent = '广知' THEN NVL(cost, 0)
                                        ELSE NVL(cost, 0) END
                                WHEN pos = '快手信息流下载' THEN NVL(cost, 0) / (1 + 0.03)
                                WHEN pos = 'oppo商店下载' THEN NVL(cost, 0) / (1 + 0.05)
                                WHEN pos = 'oppo信息流下载' THEN NVL(cost, 0) / (1 + 0.08)
                                WHEN pos = 'vivo商店下载' THEN NVL(cost, 0) / (1 + 0.05)
                                WHEN pos = 'vivo信息流下载' THEN NVL(cost, 0) / (1 + 0.08)
                                WHEN pos = '华为商店下载' THEN NVL(cost, 0) / (1 + 0.00)
                                WHEN pos = '华为ads下载' THEN NVL(cost, 0) / (1 + 0.03)
                                WHEN pos = '百度搜索或信息流下载' THEN
                                    CASE
                                        WHEN type = '自投'
                                            THEN CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.38)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.381)
                                                     ELSE NVL(cost, 0) END
                                        WHEN type = '代投'
                                            THEN CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.33)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.331)
                                                     ELSE NVL(cost, 0) END
                                        ELSE NVL(cost, 0) END
                                WHEN pos = '百度搜索下载' THEN
                                    CASE
                                        WHEN type = '自投'
                                            THEN CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.35)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.351)
                                                     ELSE NVL(cost, 0) END
                                        WHEN type = '代投'
                                            THEN CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.30)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.301)
                                                     ELSE NVL(cost, 0) END
                                        ELSE NVL(cost, 0) END
                                ELSE NVL(cost, 0) END

                   -- 20250312 汤文奇 增加脸书和谷歌的返点 脸书按4%比例按季度抵扣，谷歌支付1%服务费
                   WHEN platform = '脸书'
                       THEN NVL(cost, 0) * (1 - 0.04)
                   WHEN platform = '谷歌'
                       THEN NVL(cost, 0) * (1 + 0.01)
                   ELSE NVL(cost, 0)
                   END

           WHEN dt >= '2024-10-14' AND dt <= '2024-12-31' THEN
               -- 20241014后数据以当天和杨雯文确认的媒体政策为准
               CASE
                   -- 本地推未接入消耗
                   -- when platform = '抖音' and pos = '本地推' and open_agent rlike '创研' and type = '自投'
                   --    then nvl(cost, 0) / (1 + 0.02)
                   -- when platform = '抖音' and pos = '本地推' and open_agent rlike '成都易视创新' and type = '自投'
                   --    then nvl(cost, 0) / (1 + 0.02)
                   WHEN platform = '抖音' AND pos = '千川直播' AND open_agent RLIKE '创研' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.00)
                   WHEN platform = '抖音' AND pos = '千川直播' AND open_agent RLIKE '速及' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.01)
                   WHEN platform = '抖音' AND pos = '头条直播付费流' AND open_agent RLIKE '云漾' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.02)
                   WHEN platform = '抖音' AND pos IN ('头条直播付费流', '头条信息流', '头条信息流(广州)')
                       AND open_agent = '鲸鱼' AND type = '代投'
                       THEN CASE
                                WHEN douyin_type = '二类' THEN NVL(cost, 0) / (1 + 0.02)
                                ELSE NVL(cost, 0) / (1 + 0.01) END
                   WHEN platform = '抖音' AND pos IN ('头条直播付费流', '头条信息流', '头条信息流(广州)')
                       AND open_agent = '创研' AND type = '自投'
                       THEN CASE
                                WHEN douyin_type = '二类' THEN NVL(cost, 0) / (1 + 0.045)
                                ELSE NVL(cost, 0) / (1 + 0.015) END
                   WHEN platform = '抖音' AND pos IN ('头条直播付费流', '头条信息流', '头条信息流(广州)')
                       AND open_agent = '速及' AND type = '自投'
                       THEN CASE
                                WHEN douyin_type = '二类' THEN NVL(cost, 0) / (1 + 0.06)
                                ELSE NVL(cost, 0) / (1 + 0.035) END
                   WHEN platform = '快手' AND open_agent RLIKE '速及' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.02)
                   WHEN platform = '快手' AND open_agent RLIKE '微盟' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.05)
                   WHEN platform = '快手' AND open_agent RLIKE '微盟' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.08)
                   WHEN platform = '腾讯' AND open_agent RLIKE '微盟' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.08)
                   WHEN platform = '腾讯' AND open_agent RLIKE '微盟' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.03)
                   WHEN platform = '腾讯' AND open_agent RLIKE '京雅' AND material_agent RLIKE '京雅' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.05)
                   WHEN platform = '腾讯' AND open_agent RLIKE '京雅' AND material_agent NOT RLIKE '京雅' AND
                        type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.08)
                   WHEN platform = '百度' AND type = '代投' AND pos = '百度搜索'
                       THEN NVL(cost, 0) / (1 + 0.26)
                   WHEN platform = '百度' AND type = '自投' AND pos = '百度搜索'
                       THEN NVL(cost, 0) / (1 + 0.35)
                   WHEN platform = '百度' AND type = '代投' AND pos IN ('百度信息流', '百度信息流北京')
                       THEN NVL(cost, 0) / (1 + 0.32)
                   WHEN platform = '百度' AND type = '自投' AND pos IN ('百度信息流', '百度信息流北京')
                       THEN NVL(cost, 0) / (1 + 0.41)
                   WHEN platform = '百度' AND pos = '百度直播付费流'
                       THEN NVL(cost, 0) / (1 + 0.59)
                   WHEN platform = '腾讯公众号关注' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.08)
                   WHEN platform = '腾讯公众号关注' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.03)
                   ELSE NVL(cost, 0)
                   END

           WHEN dt <= '2024-10-13' THEN
               -- 20241013前数据不再调整
               CASE
                   -- 20240222调整
                   WHEN costid = '30999549'
                       THEN 0 --20240527快手该账户实际消耗为0
                   WHEN platform = '抖音' AND pos = '头条信息流' AND costid IN ('1780084400875661', '1783239915216971')
                       THEN IF(dt <= '2024-02-29', NVL(cost, 0) / (1 + 0.02), NVL(cost, 0) / (1 + 0.045))
                   WHEN platform = '抖音' AND type = '自投'
                       THEN CASE
                                WHEN dt < '2024-01-01' THEN NVL(cost, 0) / (1 + 0.07)
                                ELSE CASE
                                         WHEN NVL(cat, '') = '太极' AND
                                              (operation_agent LIKE '%创彩%' OR operation_agent LIKE '%迈科%')
                                             THEN NVL(cost, 0) / (1 + 0.02)
                                    --20240513新增
                                         WHEN operation_agent LIKE '%创研%'
                                             THEN NVL(cost, 0) / (1 + 0.045)
                                         ELSE NVL(cost, 0) / (1 + 0.06)
                                    END
                       END
                   --20240308调整
                   WHEN platform = '腾讯' AND type = '代投' AND operation_agent LIKE '%京雅%'
                       THEN NVL(cost, 0) / (1 + 0.04)
                   WHEN platform = '腾讯' AND type = '自投' AND operation_agent LIKE '%京雅%'
                       THEN NVL(cost, 0) / (1 + 0.05)
                   WHEN platform = '抖音' AND type = '代投' AND operation_agent LIKE '%万韬%'
                       THEN NVL(cost, 0) / (1 + 0.03) --20240522新增 代投新代理
                   WHEN platform = '抖音' AND type = '代投'
                       THEN IF(dt < '2024-01-01', NVL(cost, 0) / (1 + 0.03), NVL(cost, 0) / (1 + 0.02))
                   WHEN platform = '腾讯' AND type = '自投'
                       THEN CASE
                                WHEN dt < '2023-09-10'
                                    THEN NVL(cost, 0) / (1 + 0.12)
                                WHEN dt < '2024-06-01'
                                    THEN NVL(cost, 0) / (1 + 0.1)
                                ELSE NVL(cost, 0) / (1 + 0.08)
                       END --20240627腾讯自投返点自240601起改为8%(不含京雅自投，京雅只提供素材不开户)
                   WHEN platform = '腾讯' AND type = '代投'
                       THEN IF(dt < '2023-09-10', NVL(cost, 0) / (1 + 0.06),
                               IF(dt < '2024-01-01', NVL(cost, 0) / (1 + 0.05), NVL(cost, 0) / (1 + 0.04)))
                   WHEN platform = '快手' AND type = '自投'
                       THEN IF(dt < '2024-01-01', NVL(cost, 0) / (1 + 0.1), NVL(cost, 0) / (1 + 0.08))
                   WHEN platform = '快手' AND type = '代投'
                       THEN IF(dt < '2023-11-07', NVL(cost, 0) / (1 + 0.06),
                               IF(dt < '2024-01-01', NVL(cost, 0) / (1 + 0.05), NVL(cost, 0) / (1 + 0.02)))
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%新慧%'
                       THEN NVL(cost, 0) / (1 + 0.45) --20231110调整
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%新慧%'
                       THEN NVL(cost, 0) / (1 + 0.2)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%君子%'
                       THEN NVL(cost, 0) / (1 + 0.45) --20231213调整
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%君子%'
                       THEN NVL(cost, 0) / (1 + 0.2)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%海河%'
                       THEN NVL(cost, 0) / (1 + 0.5)
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%海河%'
                       THEN NVL(cost, 0) / (1 + 0.3)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%一周%'
                       THEN NVL(cost, 0) / (1 + 0.5)
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%一周%'
                       THEN NVL(cost, 0) / (1 + 0.3)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%鑫鼎润宏%'
                       THEN NVL(cost, 0) / (1 + 0.5) --20240105调整
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%鑫鼎润宏%'
                       THEN NVL(cost, 0) / (1 + 0.3)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%星选%'
                       THEN NVL(cost, 0) / (1 + 0.45) --20240122调整
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%星选%'
                       THEN NVL(cost, 0) / (1 + 0.2)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%赢宝%'
                       THEN NVL(cost, 0) / (1 + 0.45) --20240123调整
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%赢宝%'
                       THEN NVL(cost, 0) / (1 + 0.2)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%掌聚%'
                       THEN NVL(cost, 0) / (1 + 0.5) --20240223调整
                   WHEN platform = '百度' AND pos = '百度搜索' AND operation_agent LIKE '%掌聚%'
                       THEN NVL(cost, 0) / (1 + 0.25)
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND operation_agent LIKE '%原生泸州%'
                       THEN NVL(cost, 0) / (1 + 0.5) --20240313调整
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京') AND
                        (operation_agent LIKE '%聚胜%' OR operation_agent LIKE '%优矩%')
                       THEN CASE
                                WHEN type = '代投'
                                    THEN NVL(cost, 0) / (1 + 0.35)
                                WHEN type = '自投'
                                    THEN NVL(cost, 0) / (1 + 0.41)
                       END
                   WHEN platform = '百度' AND pos = '百度搜索' AND
                        (operation_agent LIKE '%聚胜%' OR operation_agent LIKE '%优矩%')
                       THEN CASE
                                WHEN type = '代投'
                                    THEN NVL(cost, 0) / (1 + 0.26)
                                WHEN type = '自投'
                                    THEN NVL(cost, 0) / (1 + 0.32)
                       END
                   WHEN platform = '百度' AND pos IN ('百度信息流', '百度信息流北京')
                       THEN NVL(cost, 0) / (1 + 0.4)
                   WHEN platform = '百度' AND pos = '百度搜索'
                       THEN NVL(cost, 0) / (1 + 0.25)
                   WHEN platform = '百度' AND pos = '百度直播付费流'
                       THEN NVL(cost, 0) / (1 + 0.35)
                   WHEN platform IN ('oppp', 'vivo') AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.12)
                   WHEN platform = '喜马拉雅' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.12) ---20240328 喜马拉雅渠道可以抓取到消耗，新增对应的渠道以及版位以及返点定义
                   WHEN platform = '腾讯公众号关注' AND type = '自投'
                       THEN NVL(cost, 0) / (1 + 0.08)
                   WHEN platform = '腾讯公众号关注' AND type = '代投'
                       THEN NVL(cost, 0) / (1 + 0.03) -- 20240813 新增腾讯公众号关注的返点
                   ELSE NVL(cost, 0)
                   END
    END                                                                          AS cost_real
     , dv
     , ck
     , pay_count
     , convert_cnt
     , country_code
FROM base
;