import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountStateByKey {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org")
    logger.setLevel(Level.ERROR)

    val spark = new SparkContext("local[*]", "WordCountStateByKey")
    val ssc = new StreamingContext(spark, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9997)
    val pairs = lines.flatMap(x => x.split(" ")).map(x => (x, 1))

    def reduceFunc(x: Int, y: Int) = {
      x + y
    }

    def invReduceFunc(x: Int, y: Int) = {
      x - y
    }

    val res = pairs.reduceByKeyAndWindow(reduceFunc, invReduceFunc(_, _), Seconds(30), Seconds(20))
    res.print()
    ssc.checkpoint(".")
    ssc.start()
    ssc.awaitTermination()
  }
}
