package StreamingData

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.delta.tables.DeltaTable
import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.{BooleanType, DateType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex

object TransformFunction {

  /*create hist table function*/
  def createSDCDeltaTable(sparkSession: SparkSession , dataFrame: DataFrame, deltaTableName: String, partitionCol:String): Unit = {
    val colOnlyDF = dataFrame.drop("op_type", "ds", "table", "scn", "current_ts", "row_id", "username")
    var schema:StructType = colOnlyDF.schema
    schema = schema.add(StructField("ActiveIndicator", BooleanType))
    schema = schema.add(StructField("EffectiveDate", DateType))
    schema = schema.add(StructField("EndDate", DateType))
    schema = schema.add(StructField("Action", StringType))

    DeltaTable.createIfNotExists(sparkSession)
      .addColumns(schema)
      .tableName(deltaTableName)
      .partitionedBy(partitionCol)
      .execute()

  }

  /*create sdc2 delta table function*/
  def createHistDeltaTable(sparkSession: SparkSession , dataFrame: DataFrame, deltaTableName: String, partitionCol:String): Unit = {
    val dropSchemaCol:DataFrame = dataFrame.drop("JoinKey")
    val schema:StructType = dropSchemaCol.schema

    DeltaTable.createIfNotExists(sparkSession)
      .addColumns(schema)
      .tableName(deltaTableName)
      .partitionedBy(partitionCol)
      .execute()
  }

  /*get string condition for merge historical data*/
  def createCondMergeHist(listColKey:List[String]): String ={
    var stringCond:String = ""
    for (item <- listColKey) {
      if (item != listColKey.last)
        stringCond = stringCond + s"t.$item = s.$item AND "
      else
        stringCond = stringCond + s"t.$item = s.$item"
    }
    stringCond
  }

  /*get list of columns from spark dataframe except specific columns*/
  def getListColumnFromSDF(targetDF:DataFrame, noNeedCol:List[String]): List[String] ={
    val fullListCol:List[String] = targetDF.columns.toList
    var resListCol:List[String] = List()
    for (item <- fullListCol)
      if (!noNeedCol.contains(item))
        resListCol = item :: resListCol
    resListCol.reverse
  }

  /*create string condition for detecting the update*/
  def createConditionalUpdate(joinKey:String, listCol:List[String]): String = {
    var conditionalUpdate:String = ""
    for (col <- listCol)
      if (col != joinKey) {
        if (col != listCol.last)
          conditionalUpdate = conditionalUpdate + s"delta.$col <> staged_updates.$col OR "
        else
          conditionalUpdate = conditionalUpdate + s"delta.$col <> staged_updates.$col"
      }
    conditionalUpdate
  }

  /*create map for insert new record*/
  def createMapInsertExpr(listCol: List[String]):Map[String, String] = {
    var mapValueExpr:Map[String, String] = Map()
    for (item <- listCol) {
      mapValueExpr = mapValueExpr + (item -> s"staged_updates.$item")
    }
    mapValueExpr = mapValueExpr + ("ActiveIndicator" -> "True", "EffectiveDate" -> "staged_updates.ds", "EndDate" -> "null", "Action" -> "staged_updates.op_type")
    mapValueExpr
  }

  /*function to read avro schema string from schema_registry_url and pro_schema_metadata*/
  def retrieveAvroString(schema_registry_url: String, pro_schema_metadata: String): ParsedSchema = {
    val srClient = new CachedSchemaRegistryClient(schema_registry_url, 10)
    val subjectMetadata: SchemaMetadata = srClient.getLatestSchemaMetadata(pro_schema_metadata)
    val stringAvroSchema: ParsedSchema = srClient.getSchemaById(subjectMetadata.getId)
    stringAvroSchema
  }

  /*function to parse avro string to spark struct type schema*/
  def parseStringAvro2StructType(stringAvroSchema: String): StructType = {
    val avroSchema: Schema = new Schema.Parser().parse(stringAvroSchema)
    val schemaType = SchemaConverters
      .toSqlType(avroSchema)
      .dataType
      .asInstanceOf[StructType]
    schemaType
  }

  def parseStringAvro2StructTypeDebezium(stringAvroSchema: String): StructType = {
    val firstMatch = (new Regex("""before.*after""") findFirstIn stringAvroSchema).mkString(",")
    val resMatch = (new Regex("""[{].type...record.*Value.[}]""") findFirstIn firstMatch).mkString(",")
    var parsedStructType:StructType = parseStringAvro2StructType(resMatch)
    parsedStructType = parsedStructType.add(StructField("op_type",StringType,nullable = true))
    parsedStructType = parsedStructType.add(StructField("ds",LongType,nullable = true))
    parsedStructType
  }

  /*read delta table*/
  def readDelta(sparkSession: SparkSession, tableName:String): DataFrame ={
    val sparkDF:DataFrame = sparkSession.read.format("delta").table(tableName).where("ActiveIndicator = true")
    sparkDF
  }

}
