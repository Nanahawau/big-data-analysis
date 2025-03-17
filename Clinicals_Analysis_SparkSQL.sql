-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Task 1: Analysis of Clinical Trial Data Using Spark SQL. Question to be answered below. 
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Get file and save in a variable. 
-- MAGIC
-- MAGIC file_directory = "/FileStore/tables"
-- MAGIC file_name = "/Clinicaltrial_16012025.csv"
-- MAGIC file_path = file_directory + file_name
-- MAGIC dbutils.fs.ls(file_path)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.head(file_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Convert to DF, this is so we can visualize the Schema. 
-- MAGIC
-- MAGIC df = spark.read.option("multiLine", True).option("header", True).csv(file_path)
-- MAGIC
-- MAGIC # df.printSchema()
-- MAGIC
-- MAGIC df.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create temporary view from dataframe. 
-- MAGIC
-- MAGIC df.createOrReplaceTempView ("clinicaltrials")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Question 1
-- MAGIC
-- MAGIC **List all the clinical trial types (as contained in the Type column of the data) along with their frequency, sorting the results from most to least frequent**

-- COMMAND ----------

SELECT COUNT(*) AS frequency, `Study Type` AS type 
  FROM clinicaltrials 
  GROUP BY `type` 
  ORDER BY frequency DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 2
-- MAGIC
-- MAGIC **The top 10 conditions along with their frequency (note, that the Condition column can contain multiple conditions in each row, so you will need to separate these out and count each occurrence separately)**

-- COMMAND ----------



SELECT Conditions AS conditions, COUNT(*) AS frequency
  FROM (
    SELECT explode(split(conditions, '[,|]')) AS conditions
    FROM clinicaltrials
) AS conditions
GROUP BY conditions
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 3
-- MAGIC
-- MAGIC **For studies with an end date, calculate the mean clinical trial length in months.**

-- COMMAND ----------


SELECT AVG((YEAR(`Completion Date`) - YEAR(`Start Date`)) * 12 + (MONTH(`Completion Date`) - MONTH(`Start Date`))) AS mean_trial_length 
  FROM clinicaltrials 
  WHERE `Completion Date` 
  IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 4
-- MAGIC
-- MAGIC **From the studies with a non-null completion date and a status of ‘Completed’ in
-- MAGIC the Study Status, calculate how many of these related to Diabetes each year.
-- MAGIC Display the trend over time in an appropriate visualisation. (For this you can
-- MAGIC assume all relevant studies will contain an exact match for ‘Diabetes’ or ‘diabetes’
-- MAGIC in the Conditions column.)**

-- COMMAND ----------

SELECT COUNT(*) as frequency, YEAR(`Completion Date`) as completion_year 
  FROM clinicaltrials where 
  YEAR(`Completion Date`) IS NOT NULL 
  AND `Study Status` = 'COMPLETED' 
  AND LOWER(Conditions) 
  LIKE '%diabetes%' 
  GROUP BY completion_year 
  ORDER BY completion_year ASC;
