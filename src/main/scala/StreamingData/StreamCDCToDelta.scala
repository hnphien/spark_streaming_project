package POC_U11_StreamingData

import POC_U11_StreamingData.TransformFunction._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import collection.breakOut

// noinspection SpellCheckingInspection
object StreamCDCToDelta {
  def main(args: Array[String]): Unit = {

    /*get value from cli parameters*/
    val BOOTSTRAP_SERVERS:String = args(0)
    val SCHEMA_REGISTRY_URL:String = args(1)
    val TOPICS:String = args(2)
    val JOIN_KEYS:String = args(3)
    val SDC_COLS:String = args(4)
    val PARTITION_COL = args(5)

    /*split to list TOPICS, JOIN_KEYS, SDC_COLS*/
    val listTopics:List[String] = TOPICS.split(";").map(_.trim)(breakOut)
    val listJoinKeys:List[String] = JOIN_KEYS.split(";").map(_.trim)(breakOut)
    val listSdcCols:List[String] = SDC_COLS.split(";").map(_.trim)(breakOut)
    val listPartitionCol:List[String] = PARTITION_COL.split(";").map(_.trim)(breakOut)

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

      val indexOfTopic = listTopics.indexOf(topic)

      /*read stream*/
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", s"$topic")
        .option("startingOffsets", "earliest")
        .option("kafkaConsumer.pollTimeoutMs", 120000)
        .load()

      /*get string schema avro*/
      val stringAvroSchema = retrieveAvroString(SCHEMA_REGISTRY_URL, s"$topic-value").rawSchema().toString

      /*get partition slect expr*/
      val partitionCol = listPartitionCol(indexOfTopic)
      var partitionExpr:String = ""
      if (topic.indexOf("dna") == 0) {
        partitionExpr = s"to_date(from_unixtime(data.after.$partitionCol/1000)) ${partitionCol}_PARTITION"
      } else {
        partitionExpr = s"date_from_unix_date(data.after.$partitionCol) ${partitionCol}_PARTITION"
      }

      /*get dataframe from avro*/
      var streamDF:DataFrame = spark.emptyDataFrame
      if (partitionCol == "NA") {
        streamDF = df.select(from_avro(expr("substring(value, 6, length(value)-5)"), stringAvroSchema).as("data"))
          .selectExpr(
            "data.after.*",
            "data.op",
            "to_date(from_unixtime(data.source.ts_ms/1000)) as ts_ms")
      } else {
        streamDF = df.select(from_avro(expr("substring(value, 6, length(value)-5)"), stringAvroSchema).as("data"))
          .selectExpr(
            "data.after.*",
            "data.op",
            "to_date(from_unixtime(data.source.ts_ms/1000)) as ts_ms",
            s"$partitionExpr")
      }
      streamDF = streamDF.withColumnRenamed("op", "op_type")
      streamDF = streamDF.withColumnRenamed("ts_ms", "ds")

      /*add join key for sdc type 2*/
      streamDF = streamDF.withColumn(
        "JoinKey", expr(s"concat(${listJoinKeys(indexOfTopic)
          .split(",").map(_.trim)(breakOut).map(m=>s"COALESCE($m, '')").mkString(", ")})"))

      /*generate list of columns for streaming branch 2 - SDC2 from args*/
      val listSDCCols: List[String] = listSdcCols(indexOfTopic).split(",").map(_.trim)(breakOut)
      val listJoinKey: List[String] = listJoinKeys(indexOfTopic).split(",").map(_.trim)(breakOut)
      val listUpdateOnlyCols = getListColumnFromSDF(
        streamDF, List("JoinKey", "op_type", "ds", "table", "scn", "current_ts", "row_id", "username") ::: listSDCCols ::: listJoinKey
      )

      /*create delta table if not exist*/
      val deltaBronzeTable = "[-]".r.replaceAllIn("[.].*[.]".r.replaceAllIn(topic, "."), "_")
      if (partitionCol == "NA") {
        createSDCDeltaTable(spark, streamDF, s"${deltaBronzeTable}_scd2", "EffectiveDate")
        createHistDeltaTable(spark, streamDF, s"${deltaBronzeTable}_hist", "ds")
      } else {
        createSDCDeltaTable(spark, streamDF, s"${deltaBronzeTable}_scd2", s"${partitionCol}_PARTITION")
        createHistDeltaTable(spark, streamDF, s"${deltaBronzeTable}_hist", s"${partitionCol}_PARTITION")
      }

      /*get existed delta lake - history and sdc2 table*/
      val deltaSDCTable: DeltaTable = DeltaTable.forName(spark, s"${deltaBronzeTable}_scd2")
      val deltaHisTable: DeltaTable = DeltaTable.forName(spark, s"${deltaBronzeTable}_hist")

      /*write stream to deltalake*/
      streamDF.writeStream.foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>
        {
          println(s"New batch dataframe: $topic...")
          batchDF.show(5)
          implementAllHistorical(topic, batchDF, deltaHisTable)
          implementSDC2(topic, batchDF, deltaSDCTable, "JoinKey", listSDCCols, listUpdateOnlyCols)
        }
      }
        .queryName(topic)
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("300 seconds"))
        .start()
    }

    spark.streams.awaitAnyTermination()

  }

  /*-------------------------------------------------*/
  /*--streaming branch 1: implement historical table-*/
  /*-------------------------------------------------*/
  def implementAllHistorical(topic:String, batchDF:DataFrame,deltaTable: DeltaTable): Unit ={
    /*create merge condition string from batchDF columns*/
    val listColKey = getListColumnFromSDF(batchDF, List("JoinKey", "ds", "table", "scn", "current_ts", "row_id", "username"))
    val stringCondition = createCondMergeHist(listColKey)
    val batchDFDropJoinKey:DataFrame = batchDF.drop("JoinKey")

    println(s"HIST: $topic - count rows deltaTableHist (before): ${deltaTable.toDF.count.toString}")
    deltaTable.toDF.show(5)

    deltaTable.alias("t")
      .merge(
        batchDFDropJoinKey.alias("s"), stringCondition
      )
      .whenNotMatched().insertAll()
      .execute()

    println(s"HIST: $topic - count rows deltaTableHist (after): ${deltaTable.toDF.count.toString}")
    deltaTable.toDF.show(5)
  }

  /*-------------------------------------------------*/
  /*streaming branch 2: function to create sdc type 2 */
  /*-------------------------------------------------*/
  def implementSDC2(topic:String, batchDF:DataFrame, deltaTable: DeltaTable, joinKey:String, listSDCCols:List[String], listUpdateOnlyCols:List[String]): Unit = {

    println(s"SDC: $topic - deltaSDCTable before: ${deltaTable.toDF.count.toString}")
    deltaTable.toDF.show(5)

    /*get newest record from batchDF when restart application, old records have already saved*/
    var newestBatchDF:DataFrame = batchDF
      .sort(col(joinKey).asc, col("ds").desc_nulls_last)
      .dropDuplicates(joinKey)
    val listCols:List[String] = s"JoinKey,${getListColumnFromSDF(batchDF, List("JoinKey")).mkString(",")}".split(",").map(_.trim)(breakOut)
    newestBatchDF = newestBatchDF.select(listCols.map(m=>col(m)):_*)

    /*create condition for matching*/
    val stringCondUpdate = createConditionalUpdate(joinKey, listSDCCols)
    val stringCondUpdateOnly = createConditionalUpdate(joinKey, listUpdateOnlyCols)

    /*create map for insertExpr*/
    val listColInsert = getListColumnFromSDF(newestBatchDF, List("op_type", "ds", "table", "scn", "current_ts", "row_id", "username"))
    val mapValueExpr = createMapInsertExpr(listColInsert)

    /*new records will be inserted - sdc2*/
    var newRecordToInsert: DataFrame = newestBatchDF.as("newDF")
      .join(deltaTable.toDF.as("delta"), joinKey)
      .select("newDF.*")
      .where(s"delta.ActiveIndicator = true and (${stringCondUpdate.replace("staged_updates", "newDF")})")

    newRecordToInsert = newRecordToInsert.withColumn("EffectiveDate", current_timestamp)

    /*stages of update - tmp dataframe for merging to delta lake*/
    val stageOfUpdate: DataFrame = newRecordToInsert.as("updates")
      .selectExpr("NULL as mergeKey", "updates.*")
      .union(
        newestBatchDF.as("newDF")
          .selectExpr(s"newDF.$joinKey as mergeKey", "*")
          .withColumn("EffectiveDate", current_timestamp)
      )

    /*merge deltatable*/
    val mergeDeltaTable = new MergeDeltaTable(
      deltaTable, stageOfUpdate, joinKey, stringCondUpdate, stringCondUpdateOnly, listUpdateOnlyCols, mapValueExpr
    )
    if (listSDCCols.isEmpty && listUpdateOnlyCols.nonEmpty) {
      println(s"$topic - mergeWhenUpdateOnly")
      mergeDeltaTable.mergeWhenUpdateOnly()
    }
    else if (listSDCCols.nonEmpty && listUpdateOnlyCols.isEmpty) {
      println(s"$topic - mergeWhenSDCOnly")
      mergeDeltaTable.mergeWhenSDCOnly()
    }
    else if (listSDCCols.nonEmpty && listUpdateOnlyCols.nonEmpty) {
      println(s"$topic - mergeWhenSDCAndUpdate")
      mergeDeltaTable.mergeWhenSDCAndUpdate()
    }

    println(s"SDC: $topic - deltaSDCTable after: ${deltaTable.toDF.count.toString}")
    deltaTable.toDF.show(5)
  }
}
