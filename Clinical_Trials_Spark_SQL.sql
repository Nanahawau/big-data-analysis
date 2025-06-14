-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Task 1: Analysis of Clinical Trial Data Using Spark SQL.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Introduction
-- MAGIC
-- MAGIC As a data scientist working for a pharmaceutical company, this task is to analyze clinical trial data registered in the USA in order to understand the market. A CSV file was provided named Clinicaltrial_16012025.csv. The dataset contains over 500,000 rows. To analyze this dataset due to it's size and it's other characteristics which align with big data, I will be using Spark SQL. 
-- MAGIC
-- MAGIC To analayze this csv and answer the questions, I will be doing the following things: 
-- MAGIC
-- MAGIC - Upload the file to Databricks
-- MAGIC - Convert the file to a dataframe. 
-- MAGIC - Create a view with the dataframe. 
-- MAGIC - Answer all questions using SQL query via Spark SQL. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Import and Preprocessing
-- MAGIC
-- MAGIC The first step in the process is to upload the file to the Databricks Filesystem. Databricks provides a simple way via the Dashboard to do this.
-- MAGIC After this, I previewed the file using Databricks utility command `dbutils.fs.head`. 
-- MAGIC
-- MAGIC After previewing the file, there's no much preprocessing to be done, the colums are separated by commas and the rows are separated using the special character `_\r\n_`. `\r` moves the cursor to the end of the line while `\n` moves the cursor to the next line. 
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # This first three lines saves the file path to a variable. This is done so the variable is reusable across multiple cells without having to declare it every time. This embodies a programming paridgm called DRY. DRY means do not repeat yourself. 
-- MAGIC
-- MAGIC file_directory = "/FileStore/tables"
-- MAGIC file_name = "/Clinicaltrial_16012025.csv"
-- MAGIC file_path = file_directory + file_name
-- MAGIC
-- MAGIC
-- MAGIC # This databricks utility command is used to read a limited amount of bytes from the file uploaded. This is used to preview the data and how's it's structured. This will determine how it will be converted to a dataframe. 
-- MAGIC dbutils.fs.head(file_path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The cell below converts the csv to a dataframe using the spark.read.csv method. This method accepts multiple options: 
-- MAGIC
-- MAGIC - filepath: This is the path that points to where the CSV file is in the Databricks filesystem.
-- MAGIC - header: This options ensures the first row is treated as headers containing the column names.  
-- MAGIC - inferSchema: This option ensures the data types of the columns are inferred. 
-- MAGIC - quote: Specifies the quote character to enclose values.
-- MAGIC - escape: Specifies the escape character for quotes insides quoted field. 
-- MAGIC - multiLine: Allows Spark to read multiLine 
-- MAGIC
-- MAGIC
-- MAGIC After creating the dataframe, print the schema using the printSchema method. Here are all the columns in the newly created dataframe: 
-- MAGIC
-- MAGIC - `NCT Number:string`
-- MAGIC - `Study Title:string`
-- MAGIC - `Acronym:string`
-- MAGIC - `Study Status:string`
-- MAGIC - `Conditions:string`
-- MAGIC - `Interventions:string`
-- MAGIC - `Sponsor:string`
-- MAGIC - `Collaborators:string`
-- MAGIC - `Enrollment:integer`
-- MAGIC - `Funder Type:string`
-- MAGIC - `Study Type:string`
-- MAGIC - `Study Design:string`
-- MAGIC - `Start Date:timestamp`
-- MAGIC - `Completion Date:timestamp`
-- MAGIC
-- MAGIC
-- MAGIC The show() method is used to display a sample of 5 rows. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Convert to DF, this is so we can visualize the Schema. 
-- MAGIC df = spark.read.csv(
-- MAGIC   file_path,
-- MAGIC   header=True,
-- MAGIC   inferSchema=True,
-- MAGIC   quote='"',
-- MAGIC   escape='"',
-- MAGIC   multiLine=True
-- MAGIC )
-- MAGIC
-- MAGIC df.printSchema()
-- MAGIC
-- MAGIC df.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The cell below creates a temporary named _clinicaltrials_. This view only exists in the current Spark session and it allows us to run SQL queries just like you would against a database. The method _createOrReplaceTempView_ will create a new view if a view with that name does not exist or replace the existing one, if one exists. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create or replace a temporary view from the dataframe df. 
-- MAGIC
-- MAGIC df.createOrReplaceTempView ("clinicaltrials")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Question 1
-- MAGIC
-- MAGIC **List all the clinical trial types (as contained in the Type column of the data) along with their frequency, sorting the results from most to least frequent**
-- MAGIC
-- MAGIC The query below does the following: 
-- MAGIC
-- MAGIC 1. `SELECT COUNT(*) AS frequency`: 
-- MAGIC Counts the number of study types for each group. The column is renamed as frequency. This count includes rows with null columns. To exclude the nullable columns you can do a count(`column_name`), but this won't count the column with null `Study Type`. I chose to use the `COUNT(*) `function in this way so null columns will be counted. I used it in this way for the questions that required counts. 
-- MAGIC
-- MAGIC 2. `Study Type AS study_type`:
-- MAGIC Rename the study_type column here to study_name, this follows the best practice for naming database columns. 
-- MAGIC
-- MAGIC 3. `FROM clinicaltrials`:
-- MAGIC This is the name of the temporary view created above. Meaning we are running the query on the temporary view. 
-- MAGIC
-- MAGIC 4. `GROUP BY study_type`:
-- MAGIC This groups the records by each unique `study_type`.
-- MAGIC
-- MAGIC 5. `ORDER BY frequency DESC`:
-- MAGIC This sorts the records in descending order. This means the `study_type` with the highest number of occurrence is listed first. 
-- MAGIC
-- MAGIC From the result, most of the trials done were of the study type `INTERVENTIONAL` with `399888` occurences. The result also showed that `900` trials had no study type. 

-- COMMAND ----------

SELECT COUNT(*) AS frequency, `Study Type` AS study_type 
  FROM clinicaltrials 
  GROUP BY `study_type` 
  ORDER BY frequency DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 2
-- MAGIC
-- MAGIC **The top 10 conditions along with their frequency (note, that the Condition column can contain multiple conditions in each row, so you will need to separate these out and count each occurrence separately)**
-- MAGIC
-- MAGIC
-- MAGIC The query below does the following: 
-- MAGIC
-- MAGIC 1. `SELECT Conditions AS conditions, COUNT(*) AS frequency`:
-- MAGIC This selects the `Conditions` column, renaming it to `conditions`, do a count of each conditions per group, the column is renamed to `frequency`. 
-- MAGIC
-- MAGIC 2. `FROM (
-- MAGIC     SELECT explode(split(conditions, '[-|]')) AS conditions
-- MAGIC     FROM clinicaltrials
-- MAGIC ) AS conditions`:
-- MAGIC
-- MAGIC This is a subquery above to extract the contents of the `conditions` column seeing as it contains multiple conditions. The contents look like this `type1diabetes|Type2diabetes|Pre Diabetes|Hyperinsulinism`. The first thing we do is use the split function to split content based on the special characters (|,) in the column. The split method creates an array of comma separated values. We flatten the resulting array using the explode function, all the items in the array become individual entries in the `conditions` column. The subquery is ran first before the query in 1. 
-- MAGIC
-- MAGIC 3. `GROUP BY conditions`:
-- MAGIC This groups the items by each unique conditions. 
-- MAGIC
-- MAGIC 4. `ORDER BY frequency DESC`:
-- MAGIC This sorts the records by the frequency column in descending order. This means the condition with the highest frequency is listed first. 
-- MAGIC
-- MAGIC 5. `LIMIT 10`:
-- MAGIC This returns the first 10 records after sorting them. 
-- MAGIC
-- MAGIC From the result, the condition `healthy` had the highest number of occurence with 10360 trials with that condition, HIV infections had the 10th highest occurrence with 3819 cases recorded in the trials registered. 

-- COMMAND ----------



SELECT Conditions AS conditions, COUNT(*) AS frequency
  FROM (
    SELECT explode(split(conditions, '[-|]')) AS conditions
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
-- MAGIC
-- MAGIC
-- MAGIC The query below does the following: 
-- MAGIC
-- MAGIC
-- MAGIC 1. `SELECT AVG((YEAR(Completion Date) - YEAR(Start Date)) * 12 + (MONTH(Completion Date) - MONTH(Start Date))) AS mean_trial_length 
-- MAGIC   FROM clinicaltrials `: Subtract the year component of the `Completion Date` from the year component of the `Start Date` and convert it to months. Do the same substraction for the month component of both dates and add them together. Use the AVG() function to calculate the mean and select the result as mean_trial_length. 
-- MAGIC
-- MAGIC
-- MAGIC 2. `WHERE Completion Date IS NOT NULL`:
-- MAGIC
-- MAGIC The where clause excludes rows that have `Completion Date` of null from being considered. 
-- MAGIC
-- MAGIC
-- MAGIC From the result, the mean trial length is 35.47. 
-- MAGIC
-- MAGIC

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
-- MAGIC
-- MAGIC
-- MAGIC The query below does the following: 
-- MAGIC
-- MAGIC
-- MAGIC 1. `SELECT COUNT(*) as frequency, YEAR(`Completion Date`) as completion_year 
-- MAGIC   FROM clinicaltrials`: This selects the `Completion Date` column, renaming it as `completion_year`, do a count per group, the column is renamed to frequency.
-- MAGIC
-- MAGIC
-- MAGIC 2. `where 
-- MAGIC   YEAR(`Completion Date`) IS NOT NULL `: The where clause condition excludes rows with `Completion Date` of null.
-- MAGIC
-- MAGIC 3. `AND `Study Status` = 'COMPLETED'`: This condition in the where clause ensures we only select trials with `COMPLETED` status. 
-- MAGIC
-- MAGIC 4. `AND LOWER(Conditions) 
-- MAGIC   LIKE '%diabetes%' `: This condition first converts the condition columns to lowercase and ensures that we only select trials that are related to diabetes using the LIKE function.
-- MAGIC
-- MAGIC 5. `GROUP BY completion_year `: This groups the completion_year by each unique year. 
-- MAGIC
-- MAGIC 6. `ORDER BY completion_year ASC`: This sorts the records using the completion_year column in ascending order. Which means the earliest year will be listed first. 
-- MAGIC
-- MAGIC From the result, trials related to diabetes occurred between 1989 to 2025. A bar chart has also being used to display the result. The chart shows the frequency vs the completion year of the conditions related to diabetes. 

-- COMMAND ----------

SELECT COUNT(*) as frequency, YEAR(`Completion Date`) as completion_year 
  FROM clinicaltrials where 
  YEAR(`Completion Date`) IS NOT NULL 
  AND `Study Status` = 'COMPLETED' 
  AND LOWER(Conditions) 
  LIKE '%diabetes%' 
  GROUP BY completion_year 
  ORDER BY completion_year ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Conclusion
-- MAGIC
-- MAGIC After analysis of the data from the clinical trial data registered and answering the posed questions, these are the insights gathered. 
-- MAGIC
-- MAGIC 1. The clinical trial study type with the highest number of occurence is `INTERVENTIONAL` which means interventionals are the most common clinical trials. 
-- MAGIC
-- MAGIC 2. The conditions with the highest occurence is `healthy`. This indicates that the alot of healthy people were included in the trials. 
-- MAGIC
-- MAGIC 3. The mean trial length of the clinical trial is `35.47` months which indicates the long-term nature of the trials. 
-- MAGIC
-- MAGIC 4. We checked the completion rate of diabetes in the clincial trial data set, this showed a gradual increase in the completion rate. This trend was visualized using a bar chart to show the gradual increase in completion per year. 
-- MAGIC
-- MAGIC These insights summarizes some of the characteristics of the dataset and the usefulness of Spark SQL for analyzing large and semi-structured datasets. 