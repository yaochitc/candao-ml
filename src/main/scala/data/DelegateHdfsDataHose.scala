package data

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DelegateHdfsDataHose {
  def find(session: SparkSession
           , cols: Array[Column[_]]
           , dataPath: String
           , sep: String
           , skipHead: Boolean): DataFrame = {

    val schema = StructType(cols.map(column => {
      column.isBasicType match {
        case true => StructField(column.getName, column.getType)
        case false => StructField(column.getName, StringType)
      }
    }
    ))

    var df = session.read.format("csv")
      .option("head", s"$skipHead")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
      .option("delimiter", sep)
      .schema(schema)
      .load(dataPath)

    cols.filterNot(_.isBasicType).foreach(column => {
      val stringSplitUdf = udf({
        colContent: String => column.parse(colContent)
      }, column.getType)
      df = df.withColumn(column.getName, stringSplitUdf(col(column.getName)))
    })

    df
  }
}
