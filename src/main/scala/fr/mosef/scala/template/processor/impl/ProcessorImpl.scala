package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{lit, when}

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): DataFrame = {
    //inputDF.groupBy("group_key").sum("field1")
    inputDF.groupBy("num_client").sum("montant_total_paye")
  }

  def Rename(inputDF: DataFrame): DataFrame = {
    val resultWithRenamedColumn = inputDF.withColumnRenamed("num_client", "identifiant_client")
    resultWithRenamedColumn
  }

  def createZeroColumn(dataFrame: DataFrame, columnName: String): DataFrame = {
    val zeroColumn = lit(0)
    val resultWithZeroColumn = dataFrame.withColumn(columnName, zeroColumn)
    resultWithZeroColumn
  }
}
