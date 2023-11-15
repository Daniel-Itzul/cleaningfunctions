--- process nulls

SELECT count (*) FROM analytics_cleaning_demo.cleaning_data_set;

SELECT count (*) FROM TD_getRowsWithoutMissingValues (
  ON analytics_cleaning_demo.cleaning_data_set  AS InputTable
  USING
  TargetColumns ('[quote_day:price_type]')
) AS dt;

SELECT * FROM TD_getRowsWithMissingValues (
   ON analytics_cleaning_demo.cleaning_data_set  AS InputTable
  USING
  TargetColumns ('[quote_day:price_type]')
) AS dt;

CREATE VIEW analytics_cleaning_demo.nulls_cleaned AS
SELECT * FROM TD_getRowsWithoutMissingValues (
  ON analytics_cleaning_demo.cleaning_data_set  AS InputTable
  USING
  TargetColumns ('quote_day')
) AS dt;

SELECT * FROM analytics_cleaning_demo.nulls_cleaned;

--- Relevant columns

CREATE VIEW analytics_cleaning_demo.category_summaries AS
SELECT * FROM TD_CategoricalSummary (
  ON analytics_cleaning_demo.nulls_cleaned  AS InputTable
  USING
  TargetColumns ('ticker','price_type')
) AS dt;

SELECT * FROM analytics_cleaning_demo.category_summaries;

SELECT * FROM TD_getFutileColumns(
  ON analytics_cleaning_demo.nulls_cleaned AS InputTable PARTITION BY ANY
  ON analytics_cleaning_demo.category_summaries AS categorytable DIMENSION
  USING
  CategoricalSummaryColumn('ColumnName') 
  ThresholdValue(0.7)
)As dt; 

CREATE VIEW analytics_cleaning_demo.for_analysis AS
SELECT id, quote_day, volume, price, price_type FROM analytics_cleaning_demo.nulls_cleaned;

SELECT * FROM analytics_cleaning_demo.for_analysis;

--- statistics Missing Values
CREATE TABLE analytics_cleaning_demo.missing_fitting AS (
	SELECT * FROM TD_SimpleImputeFit (
	    ON analytics_cleaning_demo.for_analysis AS InputTable
	    USING
	    ColsForStats ('volume','price')
		Stats ('median')
	) as dt
) WITH DATA;
	 
SELECT * FROM analytics_cleaning_demo.missing_fitting;

CREATE TABLE analytics_cleaning_demo.missign_fitted AS (
SELECT * FROM TD_SimpleImputeTransform (
  ON analytics_cleaning_demo.for_analysis AS InputTable
  ON analytics_cleaning_demo.missing_fitting AS FitTable DIMENSION
) AS dt
) WITH DATA;

SELECT * FROM analytics_cleaning_demo.missign_fitted;
