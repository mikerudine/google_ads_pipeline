-- Google Ads Data Cleaning and Aggregation Pipeline
-- Author: [Mike Rudine]
-- Purpose: Consolidate raw Google Ads data into a clean table for analysis

-- Insert cleaned data into the final table
INSERT INTO `[YOUR_PROJECT].[YOUR_DESTINATION_DATASET].google_ads`
WITH 

-- Ad group stats for the last 2 days with impressions > 0
agroup AS (
    SELECT 
        _DATA_DATE, 
        ad_group_id, 
        campaign_id,
        metrics_cost_micros, 
        metrics_clicks, 
        metrics_impressions
    FROM `[YOUR_PROJECT].[YOUR_GOOGLE_ADS_DATASET].ads_AdGroupStats_[YOUR_GOOGLE_ADS_ID]` cs
    WHERE metrics_impressions > 0
      AND _DATA_DATE = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
),

-- Campaign stats for the last 2 days with impressions > 0
camp AS (
    SELECT 
        _DATA_DATE, 
        campaign_id,
        metrics_cost_micros, 
        metrics_clicks, 
        metrics_impressions
    FROM `[YOUR_PROJECT].[YOUR_GOOGLE_ADS_DATASET].ads_CampaignStats_[YOUR_GOOGLE_ADS_ID]` cs
    WHERE metrics_impressions > 0
      AND _DATA_DATE = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
),

-- Keyword stats for the last 2 days with impressions > 0
keyw AS (
    SELECT 
        _DATA_DATE, 
        ad_group_criterion_criterion_id, 
        ad_group_id, 
        campaign_id,
        metrics_cost_micros, 
        metrics_clicks, 
        metrics_impressions
    FROM `[YOUR_PROJECT].[YOUR_GOOGLE_ADS_DATASET].ads_KeywordStats_[YOUR_GOOGLE_ADS_ID]` cs
    WHERE metrics_impressions > 0
      AND _DATA_DATE = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
),

-- Mapping tables to get human-readable names
key_map AS (
    SELECT DISTINCT ad_group_criterion_criterion_id, ad_group_criterion_keyword_text 
    FROM `[YOUR_PROJECT].[YOUR_GOOGLE_ADS_DATASET].ads_Keyword_[YOUR_GOOGLE_ADS_ID]`
),
ad_map AS (
    SELECT ad_group_id, ad_group_name 
    FROM `[YOUR_PROJECT].[YOUR_GOOGLE_ADS_DATASET].ads_AdGroup_[YOUR_GOOGLE_ADS_ID]`
),
camp_map AS (
    SELECT campaign_name, campaign_id 
    FROM `[YOUR_PROJECT].[YOUR_GOOGLE_ADS_DATASET].ads_Campaign_[YOUR_GOOGLE_ADS_ID]`
),

-- Combine all data, including missing ad groups or campaigns
totals AS (
    SELECT * FROM keyw
    UNION ALL
    (
        SELECT _DATA_DATE, NULL AS ad_group_criterion_criterion_id, ad_group_id, campaign_id, 
               metrics_cost_micros, metrics_clicks, metrics_impressions 
        FROM agroup 
        WHERE ad_group_id NOT IN (SELECT DISTINCT ad_group_id FROM keyw)
    )
    UNION ALL
    (
        SELECT _DATA_DATE, NULL AS ad_group_criterion_criterion_id, NULL AS adgroup_id, campaign_id, 
               metrics_cost_micros, metrics_clicks, metrics_impressions 
        FROM camp 
        WHERE campaign_id NOT IN (SELECT DISTINCT campaign_id FROM agroup)
    )
),

-- Final join with mapping tables to get names
ttm AS (
    SELECT totals.*, 
           key_map.ad_group_criterion_keyword_text, 
           ad_map.ad_group_name, 
           camp_map.campaign_name
    FROM totals
    LEFT JOIN key_map USING (ad_group_criterion_criterion_id)
    LEFT JOIN ad_map USING (ad_group_id)
    LEFT JOIN camp_map USING (campaign_id)
)

-- Select cleaned and consolidated output
SELECT DISTINCT 
    _DATA_DATE AS ad_date,
    campaign_name,
    campaign_id,
    ad_group_name AS adgroup,
    ad_group_id AS adgroup_id,
    NULL AS ad_id,
    ad_group_criterion_keyword_text AS keyword,
    ad_group_criterion_criterion_id AS keyword_id,
    metrics_impressions AS impressions,
    metrics_clicks AS clicks,
    metrics_cost_micros / 1000000 AS spend  -- convert micros to standard currency
FROM ttm;
