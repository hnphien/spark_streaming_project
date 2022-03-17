package StreamingData

import StreamingData.StreamCDCToDelta.implementAllHistorical
import StreamingData.TransformFunction._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import collection.breakOut

// noinspection SpellCheckingInspection
object StreamFSOToDelta {
  def main(args: Array[String]): Unit = {

    /*get value from cli parameters*/
    val BOOTSTRAP_SERVERS: String = args(0)
    val SCHEMA_REGISTRY_URL: String = args(1)
    val TOPICS: String = args(2)

    /*split to list TOPICS, JOIN_KEYS, SDC_COLS*/
    val listTopics: List[String] = TOPICS.split(";").map(_.trim)(breakOut)

    /*spark session*/
    val spark = SparkSession
      .builder
      .appName(s"Streaming")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println(spark)

    /*loop through all topics*/
    for (topic <- listTopics) {

      /*read stream*/
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", s"$topic")
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", "streaming_poc_u11")
        .option("kafkaConsumer.pollTimeoutMs", 120000)
        .load()

      /*get string schema avro*/
      val stringAvroSchema = retrieveAvroString(SCHEMA_REGISTRY_URL, s"$topic-value").rawSchema().toString

      /*get dataframe from avro*/
      //substring(value, 6, length(value)-5)
      val streamDF = df.select(from_avro(expr("substring(value, 6, length(value)-5)"), stringAvroSchema).as("data"))
        .selectExpr("""data.*""", "current_date() as ds")

      /*create delta table if not exist*/
      val tableName = "[-]".r.replaceAllIn("[.].*[.]".r.replaceAllIn(topic, "."), "_")
      val deltaBronzeTable = s"fso.$tableName"
      createHistDeltaTable(spark, streamDF, s"$deltaBronzeTable", "ds")
      val deltaHisTable: DeltaTable = DeltaTable.forName(spark, s"$deltaBronzeTable")

      /*write stream to deltalake*/
      streamDF.writeStream.foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>
        {
          println(s"New batch dataframe: $topic...")
          batchDF.show(1)
          implementAllHistorical(topic, batchDF, deltaHisTable)
        }
      }
        .queryName(topic)
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("300 seconds"))
        .start()
    }
    spark.streams.awaitAnyTermination()
  }
}
