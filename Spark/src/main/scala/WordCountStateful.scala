import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountStateful {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org")
    logger.setLevel(Level.ERROR)

    val spark = new SparkContext("local[*]", "wordCount")
    val ssc = new StreamingContext(spark, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9998)

    val pairs = lines.flatMap(x => x.split(" ")).map(x => (x, 1))

    def updateFunc(newvalues: Seq[Int], previousState: Option[Int]): Option[Int] = {
      Some(newvalues.sum + previousState.getOrElse(0))
    }

    val countWord = pairs.updateStateByKey(updateFunc)
    countWord.print()
    ssc.checkpoint(".")
    ssc.start()
    ssc.awaitTermination()
  }
}
