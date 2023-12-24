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
