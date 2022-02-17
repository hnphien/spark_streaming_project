package Test

import POC_U11_StreamingData.TransformFunction.{createCondMergeHist, createConditionalUpdate, createHistDeltaTable, createMapInsertExpr, createSDCDeltaTable, getListColumnFromSDF, parseStringAvro2StructType, parseStringAvro2StructTypeDebezium, retrieveAvroString}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{col, concat, current_timestamp, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import collection.breakOut

// noinspection SpellCheckingInspection
object StreamTwoTopic {
  def main(args: Array[String]): Unit = {

    /*default values for testing*/
    var BOOTSTRAP_SERVERS: String = "localhost:9092"
    var SCHEMA_REGISTRY_URL: String = "http://localhost:8081"
    var TOPICS: String = "bankserver1.bank.holding"

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

    /*read stream*/
    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", "kondor.dbo.SpotDeals")
      .option("startingOffsets", "earliest")
      .option("kafkaConsumer.pollTimeoutMs", 120000)
      .load()

    val df2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", "kondor.dbo.PurposesT")
      .option("startingOffsets", "earliest")
      .option("kafkaConsumer.pollTimeoutMs", 120000)
      .load()

    /*get string schema avro*/
    val stringAvroSchema1 = retrieveAvroString(SCHEMA_REGISTRY_URL, "kondor.dbo.SpotDeals-value").rawSchema().toString
    val stringAvroSchema2 = retrieveAvroString(SCHEMA_REGISTRY_URL, "kondor.dbo.PurposesT-value").rawSchema().toString

    /*from avro*/
    var streamDF1 = df1.select(from_avro(expr("substring(value, 6, length(value)-5)"), stringAvroSchema1).as("data"))
      .selectExpr("data.after.*", "data.op", "data.ts_ms")
    streamDF1 = streamDF1.withColumnRenamed("op", "op_type")
    streamDF1 = streamDF1.withColumnRenamed("ts_ms", "op_ts")

    var streamDF2 = df2.select(from_avro(expr("substring(value, 6, length(value)-5)"), stringAvroSchema2).as("data"))
      .selectExpr("data.after.*", "data.op", "data.ts_ms")
    streamDF2 = streamDF2.withColumnRenamed("op", "op_type")
    streamDF2 = streamDF2.withColumnRenamed("ts_ms", "op_ts")


    /*write stream to delta lake*/
    streamDF1.writeStream.foreachBatch {
      (batchDF: DataFrame, batchId: Long) =>
      {
        println("New batch dataframe...")
        batchDF.show()
      }
    }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("300 seconds"))
      .start()

    streamDF2.writeStream.foreachBatch {
      (batchDF: DataFrame, batchId: Long) =>
      {
        println("New batch dataframe...")
        batchDF.show()
      }
    }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("300 seconds"))
      .start()

    spark.streams.awaitAnyTermination()

  }

}