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
CREATE VIEW analytics_cleaning_demo.missing_fitting AS 
	SELECT * FROM TD_SimpleImputeFit (
	    ON analytics_cleaning_demo.for_analysis AS InputTable
	    USING
	    ColsForStats ('volume','price')
		Stats ('median')
	) as dt;
	 
SELECT * FROM analytics_cleaning_demo.missing_fitting;

CREATE VIEW analytics_cleaning_demo.missign_fitted AS
SELECT * FROM TD_SimpleImputeTransform (
  ON analytics_cleaning_demo.for_analysis AS InputTable
  ON analytics_cleaning_demo.missing_fitting AS FitTable DIMENSION
) AS dt;

SELECT * FROM analytics_cleaning_demo.missign_fitted;

--- Statistics Outliers
CREATE VIEW analytics_cleaning_demo.outliers_cleaning AS
SELECT * FROM TD_OutlierFilterFit (
    ON analytics_cleaning_demo.for_analysis AS InputTable
    USING
    TargetColumns ('volume','price')
    LowerPercentile (0.1)
    UpperPercentile (0.9)
    OutlierMethod ('Percentile')
    ReplacementValue ('median')
    PercentileMethod ('PercentileCont')
  ) AS dt;
  
 select * from analytics_cleaning_demo.outliers_cleaning;
 
CREATE VIEW analytics_cleaning_demo.outliers_cleaned AS
SELECT * FROM TD_OutlierFilterTransform (
  ON analytics_cleaning_demo.for_analysis AS InputTable PARTITION BY ANY
  ON analytics_cleaning_demo.outliers_cleaning AS FitTable DIMENSION
) AS dt;

select * from analytics_cleaning_demo.outliers_cleaned;


--- analysis

SELECT * FROM MovingAverage (
  ON analytics_cleaning_demo.outliers_cleaned PARTITION BY ANY ORDER by quote_day
  USING
  MAvgType ('S')
  TargetColumns ('price')
  WindowSize (3)
  IncludeFirst ('true')
) AS dt order by id;

--- auxiliaries

DROP view analytics_cleaning_demo.outliers_cleaning;
DROP view analytics_cleaning_demo.outliers_cleaned;
DROP view analytics_cleaning_demo.nulls_cleaned;
DROP view analytics_cleaning_demo.for_analysis;