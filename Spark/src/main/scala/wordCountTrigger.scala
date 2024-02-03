import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object wordCountTrigger {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("org")
    logger.setLevel(Level.ERROR)

    // default 200 partitions are created during shuffle, since in batch the data is size
    // we are setting the shuffle partition to lower number ie 3 to improve the processing time.
    val spark = SparkSession.
      builder().config("spark.sql.shuffle.partitions", 3).
      master("local[2]").appName("TriggerApplication").getOrCreate()

    val df = spark.readStream.format("socket").option("host", "localhost").option("port", 9942).load()

    val res = df.selectExpr("explode(split(value,' ')) as word").groupBy("word").count()

    val output = res.writeStream.format("console").outputMode("complete").
      option("checkpoint-location", "checkpoint-location2").trigger(Trigger.ProcessingTime("10 seconds")).start()

    output.awaitTermination()

  }
}
