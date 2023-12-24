// Databricks notebook source
// Function for reading a csv file
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType, TimestampType, LongType}

def readCSV(filePath: String, schema: StructType): DataFrame = {
  spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .option("quote", "\"")
    .schema(schema)
    .csv(filePath)
}

def getCSVPath(entity: String): String = {
  s"/fall_2023_users/piotreks/csv/v${entity}.csv.gz"
}

def getParquetPath(entity: String): String = {
  s"/fall_2023_users/piotreks/parquet/${entity}.parquet"
}

// COMMAND ----------

//Convert to parquet
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

val entity = "Posts"

var schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("AcceptedAnswerId", IntegerType, nullable = true),
    StructField("AnswerCount", IntegerType, nullable = true),
    StructField("Body", StringType, nullable = false),
    StructField("ClosedDate", TimestampType, nullable = true),
    StructField("CommentCount", IntegerType, nullable = true),
    StructField("CommunityOwnedDate", TimestampType, nullable = true),
    StructField("CreationDate", TimestampType, nullable = false),
    StructField("FavoriteCount", IntegerType, nullable = true),
    StructField("LastActivityDate", TimestampType, nullable = false),
    StructField("LastEditDate", TimestampType, nullable = true),
    StructField("LastEditorDisplayName", StringType, nullable = true),
    StructField("LastEditorUserId", IntegerType, nullable = true),
    StructField("OwnerUserId", IntegerType, nullable = true),
    StructField("ParentId", IntegerType, nullable = true),
    StructField("PostTypeId", IntegerType, nullable = false),
    StructField("Score", IntegerType, nullable = true),
    StructField("Tags", StringType, nullable = true),
    StructField("Title", StringType, nullable = true),
    StructField("ViewCount", IntegerType, nullable = true)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

df.show(10)

// COMMAND ----------

//Convert to parquet
val entity = "Badges"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("Name", StringType, nullable = false),
    StructField("UserId", IntegerType, nullable = false),
    StructField("Date", TimestampType, nullable = false)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

//Convert to parquet
val entity = "Comments"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("CreationDate", TimestampType, nullable = false),
    StructField("PostId", IntegerType, nullable = false),
    StructField("Score", IntegerType, nullable = true),
    StructField("Text", StringType, nullable = false),
    StructField("UserId", IntegerType, nullable = true)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

//Convert to parquet
val entity = "LinkTypes"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("Type", StringType, nullable = false)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

//Convert to parquet
val entity = "PostLinks"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("CreationDate", TimestampType, nullable = false),
    StructField("PostId", IntegerType, nullable = false),
    StructField("RelatedPostId", IntegerType, nullable = false),
    StructField("LinkTypeId", IntegerType, nullable = false)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

//Convert to parquet
val entity = "PostTypes"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("Type", StringType, nullable = false)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

//Convert to parquet
val entity = "Users"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("AboutMe", StringType, nullable = true),
    StructField("Age", IntegerType, nullable = true),
    StructField("CreationDate", TimestampType, nullable = false),
    StructField("DisplayName", StringType, nullable = false),
    StructField("DownVotes", IntegerType, nullable = false),
    StructField("EmailHash", StringType, nullable = true),
    StructField("LastAccessDate", TimestampType, nullable = false),
    StructField("Location", StringType, nullable = true),
    StructField("Reputation", IntegerType, nullable = false),
    StructField("UpVotes", IntegerType, nullable = false),
    StructField("Views", IntegerType, nullable = false),
    StructField("WebsiteUrl", StringType, nullable = true),
    StructField("AccountId", IntegerType, nullable = true)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

//Convert to parquet
val entity = "VoteTypes"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("Name", StringType, nullable = false)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

//Convert to parquet
val entity = "Votes"

val schema = StructType(
  Array(
    StructField("Id", IntegerType, nullable = false),
    StructField("PostId", IntegerType, nullable = false),
    StructField("UserId", IntegerType, nullable = true),
    StructField("BountyAmount", IntegerType, nullable = true),
    StructField("VoteTypeId", IntegerType, nullable = false),
    StructField("CreationDate", TimestampType, nullable = false)
  )
)

var df = readCSV(getCSVPath(entity), schema)

df.write.mode("overwrite").parquet(getParquetPath(entity))

// COMMAND ----------

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

// COMMAND ----------

//What are the top 5 tags (from the Posts table) that have the highest number of associated posts?

// Explode the Tags column to create a new row for each tag
val explodedTagsDF = postsDF
  .select("Tags")
  .filter(col("Tags").isNotNull)
  .withColumn("Tag", explode(split(col("Tags"), "><")))
  .groupBy("Tag")
  .agg(count("Tag").alias("PostCount"))
  .orderBy(desc("PostCount"))
  .limit(5)

// Display the result
explodedTagsDF.show(truncate = false)


// COMMAND ----------

//For each user, what is the average score of their comments?

// Group by UserId and calculate the average score of their comments
val averageScoreDF = commentsDF
  .groupBy("UserId")
  .agg(avg("Score").alias("AverageCommentScore"))
  .join(usersDF, commentsDF("UserId") === usersDF("Id")) // Specify the join condition explicitly
  .select("UserId", "DisplayName", "AverageCommentScore")

// Display the result
averageScoreDF.show(truncate = false)

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
