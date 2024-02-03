
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountStreaming {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org")
    logger.setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[2]").appName("WordCount").getOrCreate()

    val df = spark.readStream.format("socket").option("host", "localhost").option("port", 9994).load()

    val explodedDF = df.selectExpr("explode(split(value,' ')) as word")
    val countDF = explodedDF.groupBy("word").count()

    val res = countDF.writeStream.format("console").outputMode("complete").option("checkpoint-location", "checkpoint-location1")
      .start()

    res.awaitTermination()

  }
}
