// Databricks notebook source
// DBTITLE 1,Load All Parquet Files to A Corresponding Dataframe
// Load the DataFrames
val badgesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Badges.parquet")

val commentsDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Comments.parquet")

val postsDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Posts.parquet")

val linkTypesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/LinkTypes.parquet")

val postTypesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/PostTypes.parquet")

val postLinksDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/PostLinks.parquet")

val usersDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Users.parquet")

val votesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Votes.parquet")

val voteTypesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/VoteTypes.parquet")

// COMMAND ----------

// DBTITLE 1,1. Top 5 Active Users by Post Count and Their Most Engaged Tag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Filter Posts for questions and answers
val filteredPostsDF = postsDF
  .filter("PostTypeId = 1 OR PostTypeId = 2") // PostTypeId 1 for questions, 2 for answers

// Explode the Tags column to separate rows for each tag
val explodedPostsDF = filteredPostsDF
  .withColumn("Tag", explode(split(regexp_replace(col("Tags"), "[<>]", ","), ",")))
  .filter("Tag != ''")

// Count posts by user and tag
val userTagCountDF = explodedPostsDF
  .groupBy("OwnerUserId", "Tag")
  .agg(count("Id").alias("PostCount"))

// Find the most active tag for each user
val windowSpec = Window.partitionBy("OwnerUserId").orderBy(desc("PostCount"))
val mostActiveTagDF = userTagCountDF
  .withColumn("rank", rank().over(windowSpec))
  .filter("rank = 1")
  .drop("rank")

// Aggregate total post count by user
val totalPostsDF = filteredPostsDF
  .groupBy("OwnerUserId")
  .agg(count("Id").alias("TotalPosts"))

// Join with the Users DataFrame to get user details and most active tag
val topUsersDF = totalPostsDF
  .join(usersDF, totalPostsDF("OwnerUserId") === usersDF("Id"))
  .join(mostActiveTagDF, "OwnerUserId")
  .select("DisplayName", "TotalPosts", "Tag")

// Get the top 5 users by total post count
val top5UsersDF = topUsersDF
  .orderBy(desc("TotalPosts"))
  .limit(5)

top5UsersDF.show()

top5UsersDF.createOrReplaceTempView("top5UsersView")




// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import pandas as pd
// MAGIC
// MAGIC # Convert the Scala DataFrame to a Pandas DataFrame using the temporary view
// MAGIC result_pd = spark.table("top5UsersView").toPandas()
// MAGIC
// MAGIC # Concatenate DisplayName and Tag for each row
// MAGIC result_pd['Label'] = result_pd['DisplayName'] + ' - ' + result_pd['Tag']
// MAGIC
// MAGIC # Now result_pd is a Pandas DataFrame
// MAGIC labels = result_pd['Label'].tolist()
// MAGIC post_counts = result_pd['TotalPosts'].tolist()
// MAGIC
// MAGIC # pie chart
// MAGIC plt.figure(figsize=(8, 8))
// MAGIC plt.pie(post_counts, labels=labels, autopct='%1.1f%%', startangle=140)
// MAGIC plt.title('Top 5 Active Users by Post Count and Tag')
// MAGIC plt.show()
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// DBTITLE 1,2. Counting Annual Posts in a Forum Data Concerning Scala
import org.apache.spark.sql.functions._
import spark.implicits._

// Filter posts with 'scala' tag
val scalaPostsDF = postsDF.filter($"Tags".contains("<scala>"))

// Extract year from CreationDate and count posts
val scalaPostsByYearDF = scalaPostsDF
  .withColumn("Year", year($"CreationDate")) // Extract year
  .groupBy("Year")
  .agg(count("Id").alias("NumberOfPosts")) // Count posts per year
  .orderBy("Year") // Order by year for readability

scalaPostsByYearDF.show()

scalaPostsByYearDF.createOrReplaceTempView("scalaPostsByYearView")


// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import pandas as pd
// MAGIC
// MAGIC scalaPostsByYear_pd = spark.table("scalaPostsByYearView").toPandas()
// MAGIC
// MAGIC # Sort DataFrame by Year for better visualization
// MAGIC scalaPostsByYear_pd = scalaPostsByYear_pd.sort_values('Year')
// MAGIC
// MAGIC # Create a bar chart
// MAGIC plt.figure(figsize=(10, 6))
// MAGIC plt.bar(scalaPostsByYear_pd['Year'], scalaPostsByYear_pd['NumberOfPosts'], color='blue')
// MAGIC
// MAGIC # Add labels and title
// MAGIC plt.xlabel('Year')
// MAGIC plt.ylabel('Number of Posts')
// MAGIC plt.title('Annual Number of Posts Tagged with Scala')
// MAGIC
// MAGIC # Show the plot
// MAGIC plt.show()
// MAGIC

// COMMAND ----------

// DBTITLE 1,3. Who is the Scala Guru? - Identifying the User with the Max Score in Scala Posts and Comments
import org.apache.spark.sql.functions._
import spark.implicits._

// Filter Scala posts
val scalaPostsDF = postsDF.filter($"Tags".contains("<scala>"))

// Prepare comments related to Scala posts
val scalaCommentsDF = commentsDF
  .join(scalaPostsDF, commentsDF("PostId") === scalaPostsDF("Id"))
  .select(commentsDF("UserId").alias("CommentUserId"), commentsDF("Score").alias("CommentScore"))

// Aggregate scores by user from posts
val postScores = scalaPostsDF
  .groupBy("OwnerUserId")
  .agg(sum("Score").alias("PostScore"))

// Aggregate scores by user from comments
val commentScores = scalaCommentsDF
  .groupBy("CommentUserId")
  .agg(sum("CommentScore").alias("CommentScore"))

// Combine scores from posts and comments
val combinedScores = postScores
  .join(commentScores, postScores("OwnerUserId") === commentScores("CommentUserId"), "outer")
  .na.fill(0) // Replace null values with 0
  .withColumn("TotalScore", $"PostScore" + $"CommentScore")
  .select($"OwnerUserId", $"TotalScore")

// Find the top user
val scalaGuruDF = combinedScores
  .join(usersDF, $"OwnerUserId" === usersDF("Id"))
  .orderBy(desc("TotalScore"))
  .limit(1)
  .select("DisplayName", "Location", "TotalScore")

scalaGuruDF.collect().foreach { row =>
  val name = row.getAs[String]("DisplayName")
  val location = row.getAs[String]("Location")
  val score = row.getAs[Long]("TotalScore")
  println(s"The Scala guru was $name from $location with a total score of $score.")
}



// COMMAND ----------

// DBTITLE 1,4. Identifying Potential Polish Users
import org.apache.spark.sql.functions._
import spark.implicits._

// Define regular expression patterns
val polishCharPattern = ".*[ąężźćńłó].*"
val pytaniePattern = ".*pytanie.*"
val plDomainPattern = ".*\\.pl"
val namePattern = ".*((ski)|(czyk))$"  // Names ending with 'ski' or 'czyk'
val locationPattern = ".*(Poland|Warsaw|Cracow|Poznan|Wroclaw).*"

// Filters on usersDF
val polishUsersDF = usersDF.filter(
    $"DisplayName".rlike(polishCharPattern) || 
    $"WebsiteUrl".rlike(plDomainPattern) ||
    $"DisplayName".rlike(namePattern) ||
    $"AboutMe".rlike(pytaniePattern) ||
    $"Location".rlike(locationPattern)
)

// Filters on postsDF
val polishPostsDF = postsDF
  .filter($"Body".rlike(polishCharPattern) || $"Body".rlike(pytaniePattern))
  .select($"OwnerUserId".alias("UserId")).distinct()

// Filters on commentsDF
val polishCommentsDF = commentsDF
  .filter($"Text".rlike(polishCharPattern) || $"Text".rlike(pytaniePattern))
  .select($"UserId").distinct()

// Combine the results from posts and comments
val combinedUserIdsDF = polishPostsDF.union(polishCommentsDF).distinct()

// Join combinedUserIdsDF with usersDF to filter users and get full details
val possiblePolishUsersDF = combinedUserIdsDF
  .join(usersDF, $"UserId" === usersDF("Id"))
  .select(usersDF.columns.map(usersDF(_)) :_*) // Select all columns from usersDF

// Union with polishUsersDF and remove duplicates
val allPossiblePolishUsersDF = possiblePolishUsersDF.union(polishUsersDF).distinct()

// Count the users
val numberOfPolishUsers = allPossiblePolishUsersDF.count()

// Show the result and count
allPossiblePolishUsersDF.show()
println(s"Total number of potential Polish users: $numberOfPolishUsers")
