import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object CardsFilesStreaming {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("org")
    logger.setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[2]").
      config("spark.sql.shuffle.partitions", 3)
      .config("spark.sql.streaming.schemaInference", "true").
      appName("cards").getOrCreate()

    // read the stream from a file
    val readFile = spark.readStream.format("json").option("path", "/Users/nakumar/IdeaProjects/test/Spark/src/datafiles/").load()

    readFile.createOrReplaceTempView("cards")

    val cardsfiltered = spark.sql("select * from cards where amount > 400000")

    val result = cardsfiltered.writeStream.
      format("json").outputMode("append").option("path", "/Users/nakumar/IdeaProjects/test/Spark/src/output/")
      .option("checkpointLocation", "/Users/nakumar/IdeaProjects/test/Spark/src/checkpoint")
      .trigger(Trigger.ProcessingTime("10 Seconds"))
      .start()

    result.awaitTermination()

  }
}
