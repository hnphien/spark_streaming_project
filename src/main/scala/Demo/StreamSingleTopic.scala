package Test

import POC_U11_StreamingData.StreamCDCToDelta.implementAllHistorical
import POC_U11_StreamingData.TransformFunction.{createHistDeltaTable, createSDCDeltaTable, retrieveAvroString}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.breakOut

// noinspection SpellCheckingInspection
object StreamSingleTopic {
  def main(args: Array[String]): Unit = {

    /*get value from cli parameters*/
    val BOOTSTRAP_SERVERS = args(0)
    val SCHEMA_REGISTRY_URL = args(1)
    val TOPICS = args(2)

    /*spark session*/
    val spark = SparkSession
      .builder
      .appName(s"Stream POC U11")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println(spark)

    /*read stream*/
    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", s"$TOPICS")
      .option("startingOffsets", "earliest")
      .option("kafkaConsumer.pollTimeoutMs", 120000)
      .load()

    /*get string schema avro*/
    val stringAvroSchema1 = retrieveAvroString(SCHEMA_REGISTRY_URL, s"$TOPICS-value").rawSchema().toString

    /*from avro*/
    var streamDF1 = df1.select(from_avro(expr("substring(value, 6, length(value)-5)"), stringAvroSchema1).as("data"))
      .selectExpr("data.after.*", "data.op", "data.ts_ms")
    streamDF1 = streamDF1.withColumnRenamed("op", "op_type")
    streamDF1 = streamDF1.withColumnRenamed("ts_ms", "op_ts")
    streamDF1.printSchema()

    /**/
//    val deltaBronzeTable = ".*[.].*[.]".r.replaceAllIn(TOPICS, "test.")
//    createHistDeltaTable(spark, streamDF1, s"${deltaBronzeTable}Hist")
//    val deltaHisTable: DeltaTable = DeltaTable.forName(spark, s"${deltaBronzeTable}Hist")
//    deltaHisTable.toDF.show()

    /*write stream to delta lake*/
    streamDF1.writeStream.foreachBatch {
      (batchDF: DataFrame, batchId: Long) =>
      {
        println("New batch dataframe...")
        batchDF.show()
//        implementAllHistorical(TOPICS, batchDF, deltaHisTable)
      }
    }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("300 seconds"))
      .start()
      .awaitTermination()

  }

}