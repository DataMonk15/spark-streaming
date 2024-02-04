
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object TumblingWindow {
  def main(args: Array[String]): Unit = {

   val logger = Logger.getLogger("org")
   logger.setLevel(Level.ERROR)

   val spark = SparkSession.builder()
     .master("local[2]")
     .appName("tumblingWindow")
     .config("spark.sql.shuffle.partitions",3)
     .getOrCreate()

    val orderSchema = StructType(List(StructField("order_id",IntegerType),
      StructField("amount",IntegerType),
      StructField("order_customer_id",IntegerType),
      StructField("store_id",IntegerType),
      StructField("order_date",TimestampType)))

    val readDF = spark.readStream.format("socket")
      .option("host","localhost")
      .option("port",1235)
      .load()

    readDF.printSchema()

    val inferSchemaDF = readDF.select(from_json(col("value"),orderSchema).alias("value"))
    val flattedDF = inferSchemaDF.select("value.*").groupBy(window(col("order_date"),"15 minute"))
      .agg(sum(col("amount")).as("total_sum"))

    val windowframe = flattedDF.select(col("window.start"),col("window.end"),col("total_sum"))

    val result = windowframe.writeStream.
      format("console")
      .outputMode("update")
      .option("checkpointLocation","/Users/nakumar/IdeaProjects/test/Spark/src/checkpoint3")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    result.awaitTermination()
  }
}
