CREATE TABLE analytics_cleaning_demo.cleaning_data_set AS
(
SELECT 
    id,
    ticker,
    quote_day,
    volume,
    price,
    price_type
FROM(
LOCATION='/gs/storage.googleapis.com/clearscape_analytics_demo_data/DEMO_AIBlogSeries/cleaning_data.csv') as stocks
) WITH DATA;
