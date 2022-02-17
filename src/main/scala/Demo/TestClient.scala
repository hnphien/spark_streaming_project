package Test

import org.apache.spark.sql.SparkSession

object TestClient extends App {

  println("starting...")

  /*spark session*/
  val spark = SparkSession
    .builder
    .master("spark://m1.spark.acb.oasis:7077,m2.spark.acb.oasis:7077,m3.spark.acb.oasis:7077")
    .appName("Stream POC U11...")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  println(spark)

}
