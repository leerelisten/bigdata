CREATE TABLE IF NOT EXISTS dwd.dwd_es_activity_ngx_dt
(
    `http_host`       varchar(200) COMMENT '请求地址',
    `size`            varchar(200) COMMENT 'size',
    `body`            string COMMENT '请求体',
    `referer`         string COMMENT 'referer',
    `backtime`        string COMMENT '返回响应时间',
    `agent`           string COMMENT 'user_agent',
    `host`            string COMMENT 'host',
    `log_path`        string COMMENT '日志文件地址',
    `clientip`        string COMMENT 'clientip',
    `xff`             string COMMENT '客户端公网IP',
    `responsetime`    string COMMENT '响应时间',
    `request`         string COMMENT '请求体',
    `request_json`    string COMMENT '请求体JSON',
    `request_time`    timestamp COMMENT '请求时间',
    `version`         string COMMENT '版本',
    `status`          string COMMENT '请求状态',
    `row_json`        string COMMENT '原始json',
    `douyin_app_type` string COMMENT '抖音APP类型'
)
    COMMENT 'activity项目ngx日志'
    PARTITIONED BY (
        `dt` string COMMENT '分区日期')
    STORED AS ORC;


INSERT OVERWRITE TABLE dwd.dwd_es_activity_ngx_dt PARTITION (dt = '${datebuf}')
SELECT http_host
     , size
     , body
     , referer
     , backtime
     , agent
     , host
     , log_path
     , clientip
     , xff
     , responsetime
     , request
     , request_json
     , request_time
     , VERSION
     , status
     , row_json
     , CASE
    -- 抖音
           WHEN agent REGEXP 'news_article|NewsArticle' THEN '今日头条'
           WHEN agent LIKE '%douyinecommerce%' THEN '抖音电商'
           WHEN agent LIKE '%aweme_lite%' THEN '极速版APP'
           WHEN agent LIKE '%aweme_hotsoon%' THEN '火山视频'
           WHEN agent REGEXP 'Bytedance|ByteLocale|BytedanceWebview' THEN '抖音'

    -- 其他
           WHEN agent REGEXP 'MicroMessenger|Weixin|WeChat' THEN '微信'
           WHEN agent REGEXP 'AliBaichuan' THEN '阿里百川'
           WHEN agent LIKE '%Tangdou%' THEN '糖豆APP'
           WHEN agent LIKE '%Edg%' THEN 'Edge浏览器'
           WHEN agent LIKE '%Iqiyi%' THEN '爱奇艺'
           WHEN agent REGEXP 'baiduboxapp|baidu|Baidu' THEN '百度'
           WHEN agent REGEXP 'VivoBrowser' THEN 'vivo浏览器'
           WHEN agent REGEXP 'HeyTapBrowser' THEN 'oppo浏览器'
           WHEN agent REGEXP 'HuaweiBrowser' THEN '华为浏览器'
           WHEN agent REGEXP 'XiaoMi|MiuiBrowser' THEN '小米浏览器'
           WHEN agent REGEXP 'UCBrowser' THEN 'UC浏览器'
           WHEN agent REGEXP 'SamsungBrowser' THEN 'Samsung浏览器'
           WHEN agent REGEXP 'Instagram' THEN 'Instagram'
           WHEN agent REGEXP 'ifengnews' THEN 'ifengnews'
           WHEN agent LIKE '%QQ%' THEN 'QQ'
           ELSE NULL
    END AS douyin_app_type
FROM ods.ods_es_activity_ngx_dt
WHERE dt = '${datebuf}'
  AND http_host = 'api-h5.xiaotangketang.com'
  AND status = 200