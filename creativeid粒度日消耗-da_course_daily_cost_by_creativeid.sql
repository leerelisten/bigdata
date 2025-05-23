SET mapred.job.name="da_course_daily_cost_by_creativeid#${datebuf}";
USE da;
CREATE EXTERNAL TABLE IF NOT EXISTS da.da_course_daily_cost_by_creativeid
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
    creative_id     string COMMENT '创意id',
    creative_name   string COMMENT '创意名称',
    type            string COMMENT '投放类型',
    cost            float COMMENT '账面消耗',
    cost_real       float COMMENT '实际消耗',
    show_cnt        int COMMENT '曝光量pv',
    click_cnt       int COMMENT '点击量pv',
    pay_cnt         int COMMENT '付费(支付1元)量pv',
    convert_cnt     int COMMENT '转化(加微)量pv',
    json_data       string COMMENT 'josn格式详细数据'
)
    COMMENT '创意id粒度消耗数据'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/dw/da/da_course_daily_cost_by_creativeid';

SET hive.exec.compress.output=TRUE;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;


SET hive.execution.engine=tez;

WITH base AS
         (SELECT t1.cdate              AS dt
               , t2.d_cat              AS cat
               , t2.d_platform         AS platform
               , t2.price              AS price
               , t2.collect_phone_type AS mobile
               , t2.link_type_v1
               , t2.link_type_v2
               , t2.d_pos              AS pos
               , t2.ad_department
               , t2.open_agent         AS open_agent
               , t2.operation_agent    AS operation_agent
               , t2.material_agent     AS material_agent
               , t1.advertiser_id      AS costid
               , t1.promotion_id       AS adid
               , t1.creative_id        AS creativeid
               , t1.creative_name      AS creativename
               --, t1.type               AS sucaitype
               , t1.cost
               , t1.show_cnt           AS dv
               , t1.click_cnt          AS ck
               , t2.ad_method          AS type
               , t2.full_version_name  AS advertiser_name
               , pay_count             AS pay_count
               , convert_cnt           AS convert_cnt
               , CASE
                     WHEN cdate >= '2024-10-12'
                         AND cost_id IN (
                                         '1799179576207372', '1803724956425216', '1803724878692378', '1803724959689737',
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
                     when cdate >= '2025-04-07'
                         and cost_id = '1803724961072330'
                         then '二类'
                     when cdate >= '2025-04-23'
                         and cost_id in ('1824381427477594','1824381430018059','1824381428546041','1824381428014276')
                         THEN '二类'
                     when cdate >= '2025-04-28'
                         and cost_id = '1803725152880651'
                         THEN '二类'
                     ELSE '其他'
                 END                           AS douyin_type
               , '自运营'              AS douyin_flag
               , t1.json_data
          FROM (SELECT cdate         --抓取日期
                     , platform      --渠道
                     , advertiser_id --账户id
                     , promotion_id  --计划id
                     , creative_id   -- 素材id
                     , creative_name -- 素材名称
                     --, type          -- 素材类型
                     , cost          --账户消耗
                     , show_cnt      --曝光pv
                     , click_cnt     --点击pv
                     , pay_count     --付费(支付1元)pv
                     , convert_cnt   --转化(加微)pv
                     , json_data
                FROM ods.ods_marketing_report_creative --api抓取的原始数据
                WHERE cdate >= '2024-06-29'
                   --AND material_name <> '--' --过滤这个是基于什么原因不明确，先沿用之前的
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
               ON t1.advertiser_id = t2.cost_id)

INSERT
OVERWRITE
TABLE
da.da_course_daily_cost_by_creativeid
PARTITION
(
dt = '${datebuf}'
)
SELECT dt                                                                        AS d_date
     , cat
     , CASE WHEN platform = '腾讯pcad' THEN '腾讯' ELSE platform END             AS platform
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
     , pos
     , ad_department
     , operation_agent
     , advertiser_name
     , costid                                                                    AS cost_id
     , CONCAT_WS('-', SPLIT(advertiser_name, '-')[1], SPLIT(advertiser_name, '-')[2],
                 SPLIT(advertiser_name, '-')[3], SPLIT(advertiser_name, '-')[4]) AS agent
     , adid                                                                      AS ad_id
     , creativeid -- 20240815
     , creativename
     --, sucaitype
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
                   when platform = '百度' and dt >= '2025-04-03'
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
                                else NVL(cost, 0) end

                   when platform = '百度' and dt <= '2025-04-02'
                       THEN CASE
                                WHEN type = '代投' AND pos = '百度搜索'
                                    THEN NVL(cost, 0) / (1 + 0.26)
                                WHEN type = '自投' AND pos = '百度搜索'
                                    THEN NVL(cost, 0) / (1 + 0.35)
                                WHEN type = '代投' AND pos IN ('百度信息流', '百度信息流北京')
                                    THEN NVL(cost, 0) / (1 + 0.32)
                                WHEN type = '自投' AND pos IN ('百度信息流', '百度信息流北京')
                                    THEN NVL(cost, 0) / (1 + 0.41)
                       end

                   WHEN platform = '太极APP'
                       THEN CASE
                                WHEN pos = '抖音信息流下载' THEN
                                    case
                                        when type = '代投' then
                                            case
                                                when open_agent = '广知' and dt >= '2025-03-01' then NVL(cost, 0)
                                                when open_agent = '广知' and dt < '2025-03-01'
                                                    then NVL(cost, 0) / (1 + 0.01)
                                                when open_agent in ('云流量', '引领') then NVL(cost, 0)
                                                when open_agent = '经纬' then NVL(cost, 0) * 1.03
                                                when open_agent = '聚拓' then NVL(cost, 0) * 1.04
                                                else NVL(cost, 0)
                                                end
                                        when type = '自投' then
                                            CASE
                                                WHEN douyin_flag = '自运营' THEN NVL(cost, 0) / (1 + 0.04)
                                                WHEN douyin_flag = '走量' THEN NVL(cost, 0) / (1 + 0.02)
                                                ELSE NVL(cost, 0) / (1 + 0.00)
                                                END
                                        end
                                WHEN pos = '腾讯信息流下载' THEN
                                    CASE
                                        WHEN open_agent = '微盟' THEN NVL(cost, 0) / (1 + 0.01)
                                        WHEN open_agent = '广知' THEN NVL(cost, 0)
                                        ELSE NVL(cost, 0) END
                                WHEN pos = '快手信息流下载' THEN NVL(cost, 0) / (1 + 0.03)
                                when pos = 'oppo商店下载' then NVL(cost, 0) / (1 + 0.05)
                                when pos = 'oppo信息流下载' then NVL(cost, 0) / (1 + 0.08)
                                when pos = 'vivo商店下载' then NVL(cost, 0) / (1 + 0.05)
                                when pos = 'vivo信息流下载' then NVL(cost, 0) / (1 + 0.08)
                                when pos = '华为商店下载' then NVL(cost, 0) / (1 + 0.00)
                                when pos = '华为ads下载' then NVL(cost, 0) / (1 + 0.03)
                                when pos = '百度搜索或信息流下载' then
                                    case
                                        when type = '自投'
                                            then CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.38)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.381)
                                                     ELSE NVL(cost, 0) END
                                        when type = '代投'
                                            then CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.33)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.331)
                                                     ELSE NVL(cost, 0) END
                                        else NVL(cost, 0) end
                                when pos = '百度搜索下载' then
                                    case
                                        when type = '自投'
                                            then CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.35)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.351)
                                                     ELSE NVL(cost, 0) END
                                        when type = '代投'
                                            then CASE
                                                     WHEN open_agent IN ('聚创', '优矩') THEN NVL(cost, 0) / (1 + 0.30)
                                                     WHEN open_agent = '爱德' THEN NVL(cost, 0) / (1 + 0.301)
                                                     ELSE NVL(cost, 0) END
                                        else NVL(cost, 0) end
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
     , dv                                                                        AS show_cnt
     , ck                                                                        AS click_cnt
     , pay_count                                                                    pay_cnt
     , convert_cnt
     , json_data
FROM base
WHERE dt >= '2024-06-29'

