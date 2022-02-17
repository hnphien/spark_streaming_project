package POC_U11_StreamingData

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame

class MergeDeltaTable(deltaTable:DeltaTable,
                      stageOfUpdate:DataFrame,
                      joinKey:String,
                      stringCondUpdate:String,
                      stringCondUpdateOnly:String,
                      listUpdateOnlyCols:List[String],
                      mapValueExpr:Map[String, String]) {

  def mergeWhenSDCAndUpdate(): Unit ={
    deltaTable
      .as("delta")
      .merge(
        stageOfUpdate.as("staged_updates"), s"delta.$joinKey = mergeKey")
      /*for update op_type*/
      .whenMatched(s"delta.ActiveIndicator = true AND ($stringCondUpdate)")
      .updateExpr(Map(
        "ActiveIndicator" -> "False", "EndDate" -> "staged_updates.ds", "Action" -> "staged_updates.op_type"
      ))
      /*for delete op_type*/
      .whenMatched(s"delta.ActiveIndicator = true AND staged_updates.op_type = 'd'")
      .updateExpr(Map(
        "ActiveIndicator" -> "False", "EndDate" -> "staged_updates.ds", "Action" -> "staged_updates.op_type"
      ))
      /*update directly into table - do not save with sdc2*/
      .whenMatched(s"($stringCondUpdateOnly)")
      .updateExpr(Map(
        listUpdateOnlyCols map {s => (s, s"staged_updates.$s")} : _*
      ))
      /*for insert op_type*/
      .whenNotMatched()
      .insertExpr(mapValueExpr)
      .execute()
  }

  def mergeWhenSDCOnly(): Unit ={
    deltaTable
      .as("delta")
      .merge(
        stageOfUpdate.as("staged_updates"), s"delta.$joinKey = mergeKey")
      /*for update op_type*/
      .whenMatched(s"delta.ActiveIndicator = true AND ($stringCondUpdate)")
      .updateExpr(Map(
        "ActiveIndicator" -> "False", "EndDate" -> "staged_updates.ds", "Action" -> "staged_updates.op_type"
      ))
      /*for delete op_type*/
      .whenMatched(s"delta.ActiveIndicator = true AND staged_updates.op_type = 'd'")
      .updateExpr(Map(
        "ActiveIndicator" -> "False", "EndDate" -> "staged_updates.ds", "Action" -> "staged_updates.op_type"
      ))
      /*for insert op_type*/
      .whenNotMatched()
      .insertExpr(mapValueExpr)
      .execute()
  }

  def mergeWhenUpdateOnly(): Unit ={
    deltaTable
      .as("delta")
      .merge(
        stageOfUpdate.as("staged_updates"), s"delta.$joinKey = mergeKey")
      /*for delete op_type*/
      .whenMatched(s"delta.ActiveIndicator = true AND staged_updates.op_type = 'd'")
      .updateExpr(Map(
        "ActiveIndicator" -> "False", "EndDate" -> "staged_updates.ds", "Action" -> "staged_updates.op_type"
      ))
      /*update directly into table - do not save with sdc2*/
      .whenMatched(s"($stringCondUpdateOnly)")
      .updateExpr(Map(
        listUpdateOnlyCols map {s => (s, s"staged_updates.$s")} : _*
      ))
      /*for insert op_type*/
      .whenNotMatched()
      .insertExpr(mapValueExpr)
      .execute()
  }

}
