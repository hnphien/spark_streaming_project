package Test

import org.apache.spark.sql.functions.col

import scala.collection.breakOut
import scala.util.matching.Regex

// noinspection SpellCheckingInspection
object Demo extends App {

//  val topic = "kondor-1"
//  var name:String = "[-]".r.replaceAllIn("[.].*[.]".r.replaceAllIn(topic, "."), "_")
//  println(s"$name")

//  val str = "NGAY,ACCTNBR,PERSNBR,ORGNBR,SBT,CASHBOXNBR,CASHBOXTXNNBR"
//  val res:String = str.split(",").map(_.trim)(breakOut).map(m=>s"COALESCE($m, '')").mkString(", ")
//  println(res)

  val listDFCol:List[String] = List("A", "B", "C")
  val partitionDateCol = "B"

  println(listDFCol.map(m=>m))

}
