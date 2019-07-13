package PubNative

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait PubNativeSpark {

  val spark = SparkSession.builder().appName("PubNative").master("local[8]").getOrCreate()
  import spark.implicits._

  /**
    * Read the json based on a path and inferring its schema
    * @param path to the json file
    * @return a Spark Dataframe
    */
  def readJSON(path: String): DataFrame = {
    spark.read.option("inferSchema", "true").json(path)
  }

  /**
    * Get the top five recommendations
    * @param metrics the dataframe containing all the metrics
    * @return A Dataframe containing the recommended advisor Ids by country code and App id
    */
  def generateRecommendations(metrics: DataFrame): DataFrame = {
    val recommendationWindow = Window.partitionBy("app_id", "country_code").orderBy($"metrics".desc)
    metrics.withColumn("metrics", ($"revenue"/$"impressions").cast("Double"))
      .na.drop(Seq("metrics"))
      .select($"app_id", $"country_code", $"advertiser_id", $"metrics", rank() over recommendationWindow as "rank")
      .where(!$"country_code".isNull && $"country_code" =!= "").where($"rank" <= 5)
      .withColumn("recommended_advertiser_ids", collect_set($"advertiser_id") over recommendationWindow)
      .drop("rank", "advertiser_id", "metrics").repartition(1)
  }

  /**
    * Write a dataframe to a single JSON file
    * @param df dataframe
    * @param path output path to which JSON file has to be written to
    */
  def write(df: DataFrame, path: String ): Unit = {
    df.write.format("org.apache.spark.sql.json").mode("overwrite").json(path)
  }
}
