package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions}

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    //inputDF.groupBy("group_key").sum("field1")
    inputDF.groupBy("num_client").sum("montant_total_paye")
  }
  def countRows(dataFrame: DataFrame): DataFrame = {
    val rowCount = dataFrame.count()
    val spark = dataFrame.sparkSession
    import spark.implicits._
    val countDF = Seq(rowCount).toDF("rowCount")
    countDF
  }

}
