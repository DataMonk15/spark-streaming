import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object RunningTotalStreaming {
  def main(args: Array[String]): Unit = {
   val logger = Logger.getLogger("org")
    logger.setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("runningTotal")
      .config("spark.sql.streaming.schemaInference","true")
      .config("spark.sql.shuffle.partitions",3)
      .master("local[2]")
      .getOrCreate()

    val resultDF = spark.readStream.format("json")
      .option("path","/Users/nakumar/IdeaProjects/test/Spark/src/datafiles/")
      .load()

    resultDF.createOrReplaceTempView("cards")

    val result = spark.sql("select sum(amount) from cards where card_id='5572427538311236'")

    val DFres = result.writeStream.format("console")
      .outputMode("complete")
      .option("checkpointLocation","/Users/nakumar/IdeaProjects/test/Spark/src/checkpoint2")
      .trigger(Trigger.ProcessingTime("10 Seconds"))
      .start()

    DFres.awaitTermination()

  }
}
