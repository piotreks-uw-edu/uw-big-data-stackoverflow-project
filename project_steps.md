@echo off
setlocal enabledelayedexpansion

set BCP_EXPORT_SERVER=localhost
set BCP_EXPORT_DB=StackOverflow2010

rem Define a list of user tables
rem set "TABLES=Badges Comments LinkTypes PostLinks PostTypes Posts Users VoteTypes Votes"
set "TABLES=LinkTypes PostTypes"

rem Iterate through each table in the list
for %%T in (%TABLES%) do (
    set BCP_EXPORT_TABLE=%%T

    BCP "DECLARE @colnames VARCHAR(max);SELECT @colnames = COALESCE(@colnames + ',', '') + column_name from %BCP_EXPORT_DB%.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='!BCP_EXPORT_TABLE!'; select @colnames;" queryout D:\csv\HeadersOnly.csv -c -T -S%BCP_EXPORT_SERVER%

    BCP %BCP_EXPORT_DB%.dbo.!BCP_EXPORT_TABLE! out D:\csv\TableDataWithoutHeaders.csv -c -t, -T -S%BCP_EXPORT_SERVER%

    copy /b D:\csv\HeadersOnly.csv+D:\csv\TableDataWithoutHeaders.csv D:\csv\!BCP_EXPORT_TABLE!.csv

    del D:\csv\HeadersOnly.csv
    del D:\csv\TableDataWithoutHeaders.csv
)

endlocal
pause


### run docker
docker run -it --name databricks-cli --volume D:\csv:/home/csv python:latests bash

databricks configure --token

https://adb-6199578147525915.15.azuredatabricks.net/
dapie0c8515fe78ee9ff4b71ab818266b177


### copy the csv files to "dbfs:/fall_2023_users/piotreks/"
pip install databricks-cli

databricks fs cp -r /home/csv dbfs:/fall_2023_users/piotreks/ --overwrite












Tasks:
1. How many users are there in the database

SELECT COUNT(*) FROM Users

2. How many users declared being from Seattle

WITH Seattle AS(
	SELECT Location, COUNT(*) cnt
	FROM dbo.Users
	WHERE Location LIKE '%Seattle%'
	GROUP BY Location
)
SELECT SUM(cnt) from Seattle

3. Top Contributors: Who are the top 5 users based on the number of posts they have authored? Consider both questions and answers in the Posts table.

4. Most Discussed Topics: What are the top 5 tags (from the Posts table) that have the highest number of associated posts?

5. User Engagement: For each user, what is the average score of their comments? Use the Comments table for this analysis.

6. Popular Posts: What are the top 3 posts with the highest view count that also have more than 10 comments? This will require a join between the Posts and Comments tables.

7. Expertise Areas: Which users have answered the most questions tagged with 'c#' or '.net'? This will involve filtering and counting answers in the Posts table based on tags.

8. Community Response Time: On average, how long does it take for a question to receive its first comment? This requires comparing the creation times of questions and their first comment.

9. Influential Posts: What are the 5 posts with the highest number of linked posts? Use the PostLink table for this, considering the 'Linked' link type.

10. User Sentiment Analysis: Which users have the highest and lowest average comment scores? This analysis can indicate the perceived helpfulness or quality of user contributions in comments.