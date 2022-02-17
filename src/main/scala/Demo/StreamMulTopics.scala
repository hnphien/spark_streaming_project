package Test

import POC_U11_StreamingData.TransformFunction.retrieveAvroString
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.breakOut

// noinspection SpellCheckingInspection
object StreamMulTopics {
  def main(args: Array[String]): Unit = {

    /*default values for testing*/
    var BOOTSTRAP_SERVERS: String = "localhost:9092"
    var SCHEMA_REGISTRY_URL: String = "http://localhost:8081"
    var TOPICS: String = "kondor.dbo.CptyDefT,kondor.dbo.FloatingRates,kondor.dbo.FloatingRatesValues,kondor.dbo.Folders,kondor.dbo.ForwardDeals,kondor.dbo.PairsDef,kondor.dbo.PurposesT,kondor.dbo.SpotDeals"

    /*get value from cli parameters*/
    if (args.length > 0) {
      BOOTSTRAP_SERVERS = args(0)
      SCHEMA_REGISTRY_URL = args(1)
      TOPICS = args(2)
    }

    /*spark session*/
    val spark = SparkSession
      .builder
      .appName(s"Stream POC U11")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println(spark)

    /*get list of topic*/
    val listSDCCols: List[String] = TOPICS.split(",").map(_.trim)(breakOut)

    /*loop through all topic*/
    for (topic <- listSDCCols) {
      /*read stream*/
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("kafkaConsumer.pollTimeoutMs", 120000)
        .load()

      /*get string schema avro*/
      val stringAvroSchema = retrieveAvroString(SCHEMA_REGISTRY_URL, s"$topic-value").rawSchema().toString

      /*from avro*/
      var streamDF = df.select(from_avro(expr("substring(value, 6, length(value)-5)"), stringAvroSchema).as("data"))
        .selectExpr("data.after.*", "data.op", "data.ts_ms")
      streamDF = streamDF.withColumnRenamed("op", "op_type")
      streamDF = streamDF.withColumnRenamed("ts_ms", "op_ts")

      /*write stream to delta lake*/
      streamDF.writeStream.foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>
        {
          println(s"New batch dataframe: $topic...")
          batchDF.show()
        }
      }
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("300 seconds"))
        .start()
    }

    spark.streams.awaitAnyTermination()

  }

}