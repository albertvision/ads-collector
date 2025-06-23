CREATE TABLE IF NOT EXISTS ads_data (
    account_type VARCHAR(20),
    account_id BIGINT,
    campaign_id BIGINT,
    campaign_name VARCHAR(255),
    adset_id BIGINT,
    adset_name VARCHAR(255),
    ad_id BIGINT,
    ad_name VARCHAR(255),
    spend DOUBLE,
    impressions INT,
    clicks INT,
    date DATE,
    PRIMARY KEY (ad_id, date)
);
