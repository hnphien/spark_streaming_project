package Test

import io.delta.tables.DeltaTable
import org.apache.spark.rdd
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object HiveMetastore extends App {

  val spark = SparkSession
    .builder
    .appName(s"Query POC U11")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  println(spark)

//  spark.sql("CREATE DATABASE IF NOT EXISTS kondor")
//  spark.sql("CREATE DATABASE IF NOT EXISTS dna")
//  spark.sql("CREATE DATABASE IF NOT EXISTS test")

//  spark.sql("DROP DATABASE IF EXISTS test CASCADE")
//  spark.sql("DROP DATABASE IF EXISTS dna CASCADE")
//  spark.sql("DROP DATABASE IF EXISTS kondor CASCADE")

  val dfDbs = spark.sql("SHOW DATABASES")
  dfDbs.show()

//  val df = spark.sql("select * from test.CptyDefTEventHist")
//  df.show()

}
