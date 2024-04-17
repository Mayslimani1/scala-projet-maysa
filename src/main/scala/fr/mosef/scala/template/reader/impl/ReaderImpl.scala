package fr.mosef.scala.template.reader.impl
import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {
  val properties: Properties = new Properties()
  properties.load(new FileInputStream(propertiesFilePath))

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

//lire des données avec des options personnalisées pour le format spécifié.

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep",  properties.getProperty("read_separator"))
      .option("inferSchema", properties.getProperty("schema"))
      .option("header", properties.getProperty("read_header"))
      .format(properties.getProperty("read_format_csv"))
      .load(path)
  } // Lecture fichier CSV avec séparateur virgule (,),'inférence automatique du schéma et l'en-tête de colonne

  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet"))
      .load(path)
  }
  def readTable(tableName: String, location: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet"))
      .option("basePath", location)
      .load(location + "/" + tableName)

      }

      def read(): DataFrame = {
        sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
      } // retourne un DataFrame vide avec un message indiquant qu'il s'agit d'un DataFrame vide


}
