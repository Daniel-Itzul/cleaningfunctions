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