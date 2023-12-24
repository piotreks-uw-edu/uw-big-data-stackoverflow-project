// Databricks notebook source
// DBTITLE 1,Load All Parquet Files to A Corresponding Dataframe
// Load the DataFrames
val badgesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Badges.parquet")
println("badgesDF:")
badgesDF.show(10)

val commentsDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Comments.parquet")
println("commentsDF:")
commentsDF.show(10)

val postsDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Posts.parquet")
println("postsDF:")
postsDF.show(10)

val linkTypesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/LinkTypes.parquet")
println("linkTypesDF:")
linkTypesDF.show(10)

val postTypesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/PostTypes.parquet")
println("PostTypes dataframe:")
postTypesDF.show(10)

val postLinksDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/PostLinks.parquet")
println("postTypesDF:")
postLinksDF.show(10)

val usersDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Users.parquet")
println("usersDF:")
usersDF.show(10)

val votesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/Votes.parquet")
println("votesDF:")
votesDF.show(10)

val voteTypesDF = spark.read.parquet("dbfs:/fall_2023_users/piotreks/parquet/VoteTypes.parquet")
println("voteTypesDF:")
voteTypesDF.show(10)




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
// MAGIC # Create a pie chart
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

// Assuming postsDF is your DataFrame containing the posts data
// Filter posts with 'scala' tag
val scalaPostsDF = postsDF.filter($"Tags".contains("<scala>"))

// Extract year from CreationDate and count posts
val scalaPostsByYearDF = scalaPostsDF
  .withColumn("Year", year($"CreationDate")) // Extract year
  .groupBy("Year")
  .agg(count("Id").alias("NumberOfPosts")) // Count posts per year
  .orderBy("Year") // Order by year for readability

// Show the result
scalaPostsByYearDF.show()

scalaPostsByYearDF.createOrReplaceTempView("scalaPostsByYearView")


// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import pandas as pd
// MAGIC
// MAGIC # Retrieve the DataFrame from the temporary view
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

//Which users have answered the most questions tagged with 'c#' or '.net'?

// Filter for answers related to 'c#' or '.net'
val filteredAnswersDF = postsDF
  //.filter("PostTypeId = 2") // PostTypeId 2 for answers
  .filter("Tags LIKE '%<c#>%' OR Tags LIKE '%<.net>%'")

// Group by OwnerUserId and count the number of answers
val topAnswerersDF = filteredAnswersDF
  .groupBy("OwnerUserId")
  .agg(count("Id").alias("AnswerCount"))
  .orderBy(desc("AnswerCount"))

// Display the result
topAnswerersDF.show(truncate = false)

// COMMAND ----------

//What are the top 3 posts with the highest view count that also have more than 10 comments?

// Join Posts and Comments on PostId
val joinedDF = postsDF.join(commentsDF, postsDF("Id") === commentsDF("PostId"), "inner")

// Group by PostId and count comments
val postCommentCountDF = joinedDF.groupBy(postsDF("Id"), postsDF("ViewCount"))
  .agg(count("CommentCount").alias("CommentCount"))
  .filter("CommentCount > 10")

// Order by ViewCount in descending order and limit to the top 3 posts
val topPostsDF = postCommentCountDF.orderBy(desc("ViewCount")).limit(3)

// Display the result
topPostsDF.show(truncate = false)

// COMMAND ----------

//How many questions adked for scala?

// Filter for answers related to 'scala'
val filteredAnswersDF = postsDF
  .filter("Title LIKE '%scala%'")

// Group by OwnerUserId and count the number of answers
val topAnswerersDF = filteredAnswersDF
  .groupBy("OwnerUserId")
  .agg(count("Id").alias("AnswerCount"))
  .orderBy(desc("AnswerCount"))

// Display the result
topAnswerersDF.show(truncate = false)

// COMMAND ----------

//On average, how long does it take for a question to receive its first comment?

// Filter for questions and their first comments
val questionsWithFirstCommentDF = postsDF
  .filter("PostTypeId = 1") // PostTypeId 1 for questions
  .join(
    commentsDF
      .filter("PostId IS NOT NULL")
      .groupBy("PostId")
      .agg(min("CreationDate").alias("FirstCommentDate")),
    postsDF("Id") === commentsDF("PostId"), // Corrected join condition
    "left_outer"
  )

// Calculate the time difference between question creation and first comment
val timeDiffDF = questionsWithFirstCommentDF
  .select(
    col("Id"),
    col("CreationDate").alias("QuestionCreationDate"),
    col("FirstCommentDate"),
    datediff(col("FirstCommentDate"), col("CreationDate")).alias("DaysToFirstComment")
  )

// Display the average time to receive the first comment
val avgTimeToFirstComment = timeDiffDF.agg(avg("DaysToFirstComment").alias("AvgDaysToFirstComment"))
avgTimeToFirstComment.show(truncate = false)

// COMMAND ----------

//What are the 5 posts with the highest number of linked posts?

// Filter for posts with 'Linked' link type
val linkedPostsDF = postLinksDF
  .filter("LinkTypeId = 1") // LinkTypeId 1 for 'Linked'

// Count the number of linked posts for each post
val linkedPostCountsDF = linkedPostsDF
  .groupBy("PostId")
  .agg(count("RelatedPostId").alias("LinkedPostCount"))

// Find the top 5 posts with the highest number of linked posts
val topLinkedPostsDF = linkedPostCountsDF
  .orderBy(desc("LinkedPostCount"))
  .limit(5)

// Display the results
topLinkedPostsDF.show(truncate = false)

// COMMAND ----------

//Which users have the highest and lowest average comment scores?

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Assuming you have commentsDF DataFrame loaded

// Group by UserId and calculate the average score of their comments
val averageScoreDF = commentsDF
  .groupBy("UserId")
  .agg(avg("Score").alias("AverageCommentScore"))

// Define a window specification to rank users based on average comment score
val windowSpec = Window.orderBy(desc("AverageCommentScore"))

// Add a rank column to the DataFrame based on the average comment score
val rankedUsersDF = averageScoreDF
  .withColumn("Rank", rank().over(windowSpec))

// Display users with the highest and lowest average comment scores
val highestAverageScoreUsers = rankedUsersDF
  .filter("Rank = 1")
  .select("UserId", "AverageCommentScore")
  .show(truncate = false)

val lowestAverageScoreUsers = rankedUsersDF
  .filter(s"Rank = ${rankedUsersDF.select(max("Rank")).collect()(0)(0)}")
  .select("UserId", "AverageCommentScore")
  .show(truncate = false)


// COMMAND ----------

//How many users are there in the database



// COMMAND ----------

//How many users declared being from Seattle

// COMMAND ----------

postsDF

postsDF
  .filter(col("Title").isNotNull)
  .select("Id", "Title", "AnswerCount", "Tags", "ViewCount")
  .show(10, truncate = false)
