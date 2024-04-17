package fr.mosef.scala.template.writer
import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.sql.DataFrame


class Writer(propertiesFilePath: String) {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def write(df: DataFrame, mode: String = "overwrite", path: String, separator: String = ","): Unit = {
    df
      .write
      .option("header", properties.getProperty("write_header")) // 1ere ligne = nom des colonnes
      .option("sep", properties.getProperty("read_separator"))
      .mode(mode)
      .csv(path)
  }

  def writeParquet(df: DataFrame, mode: String = "overwrite", path: String): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }
  def writeTable(df: DataFrame, tableName: String, mode: String = "overwrite", tablePath: String): Unit = {
    df.write
      .mode(mode)
      .option("path", tablePath)
      .saveAsTable(tableName) // enregristre sous forme table Hive
  }
}