import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org")
    logger.setLevel(Level.ERROR)

    val spark = new SparkContext("local[*]", "WordCount")
    val ssc = new StreamingContext(spark, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
