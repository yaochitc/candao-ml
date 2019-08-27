import java.lang.Long
import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Candao {
  def main(args: Array[String]): Unit = {
    val dataHose = CandaoUtil.getCandao
    val session = TrainingUtil.getSparkSession

    var df = dataHose.findTrain(session)

    val parseDate = udf({
      colContent: String =>
        val year = Integer.parseInt(colContent.substring(0, 4))
        val month = Integer.parseInt(colContent.substring(5, 6))
        val day = Integer.parseInt(colContent.substring(7, colContent.length - 1))
        new Timestamp(year - 1900, month - 1, day, 8, 0, 0, 0)
    }, TimestampType)

    val parseTemp = udf({
      colContent: String =>
        Integer.parseInt(colContent.replaceAll("℃", ""))
    }, IntegerType)

    val parseAmount = udf({
      colContent: String =>
        Long.parseLong(colContent.replaceAll(",", ""))
    }, LongType)

    df = df.withColumn("date", parseDate(col("date")))
      .withColumn("dayTemp", parseTemp(col("dayTemp")))
      .withColumn("nightTemp", parseTemp(col("nightTemp")))
      .withColumn("amount1", parseAmount(col("amount1")))


    var structType = new StructType()
    structType = structType.add("date", DataTypes.TimestampType, false)
      .add("dayWeather", DataTypes.StringType, false)
      .add("nightWeather", DataTypes.StringType, false)
      .add("dayTemp", DataTypes.IntegerType, false)
      .add("nightTemp", DataTypes.IntegerType, false)
      .add("wind", DataTypes.StringType, false)
      .add("isWorkday", DataTypes.StringType, false)
      .add("isHoliday", DataTypes.StringType, false)
      .add("dish", DataTypes.StringType, false)
      .add("amount", DataTypes.LongType, false)


    df = df.flatMap(r => {
      Iterator(Row(r.getTimestamp(0),
        r.getString(1),
        r.getString(2),
        r.getInt(3),
        r.getInt(4),
        r.getString(5),
        r.getString(6),
        r.getString(7),
        r.getString(8),
        r.getLong(9)
      ), Row(r.getTimestamp(0),
        r.getString(1),
        r.getString(2),
        r.getInt(3),
        r.getInt(4),
        r.getString(5),
        r.getString(6),
        r.getString(7),
        r.getString(10),
        r.getLong(11)
      ), Row(r.getTimestamp(0),
        r.getString(1),
        r.getString(2),
        r.getInt(3),
        r.getInt(4),
        r.getString(5),
        r.getString(6),
        r.getString(7),
        r.getString(12),
        r.getLong(13)
      ))
    })(RowEncoder(structType))

    val monthOfYear = udf({
      timestamp: Timestamp =>
        timestamp.getMonth
    }, IntegerType)

    val dayOfWeek = udf({
      timestamp: Timestamp =>
        timestamp.getDay
    }, IntegerType)

    df = df.withColumn("monthOfYear", monthOfYear(col("date")))
      .withColumn("dayOfWeek", dayOfWeek(col("date")))

    val getDate = udf({
      window: GenericRowWithSchema =>
        val end = window.getTimestamp(1)
        new Timestamp(end.getTime + 24 * 3600 * 1000)
    }, TimestampType)

    val df3Days = df.groupBy(col("dish"), window(col("date"), "3 days", "1 day"))
      .agg(
        expr(s"sum(amount) as sumLast3days"),
        expr(s"mean(amount) as meanLast3days")
      ).withColumn("date", getDate(col("window")))
      .select("date", "dish", "sumLast3days", "meanLast3days")

    val df7Days = df.groupBy(col("dish"), window(col("date"), "7 days", "1 day"))
      .agg(
        expr(s"sum(amount) as sumLast7days"),
        expr(s"mean(amount) as meanLast7days")
      ).withColumn("date", getDate(col("window")))
      .select("date", "dish", "sumLast7days", "meanLast7days")

    val df15Days = df.groupBy(col("dish"), window(col("date"), "15 days", "1 day"))
      .agg(
        expr(s"sum(amount) as sumLast15days"),
        expr(s"mean(amount) as meanLast15days")
      ).withColumn("date", getDate(col("window")))
      .select("date", "dish", "sumLast15days", "meanLast15days")

    df = df.join(df3Days, Array("date", "dish"))
      .join(df7Days, Array("date", "dish"))
      .join(df15Days, Array("date", "dish"))

    val pipeline = new Pipeline()
    val stages = Array(
      new StringIndexer().setInputCol("dayWeather").setOutputCol("dayWeather_index"),
      new StringIndexer().setInputCol("nightWeather").setOutputCol("nightWeather_index"),
      new StringIndexer().setInputCol("wind").setOutputCol("wind_index"),
      new StringIndexer().setInputCol("isWorkday").setOutputCol("isWorkday_index"),
      new StringIndexer().setInputCol("isHoliday").setOutputCol("isHoliday_index")
    )

    pipeline.setStages(stages)
    df = pipeline.fit(df).transform(df)

    df = df.select("date",
      "dayTemp",
      "nightTemp",
      "monthOfYear",
      "dayOfWeek",
      "sumLast3days",
      "meanLast3days",
      "sumLast7days",
      "meanLast7days",
      "sumLast15days",
      "meanLast15days",
      "dayWeather_index",
      "nightWeather_index",
      "wind_index",
      "isWorkday_index",
      "isHoliday_index",
      "amount"
    )

    val trainDF = df.filter("date <= '2019-08-01'")
    val evalDF = df.filter("date > '2019-08-01'")

    trainDF.show()

//    val prop = new Properties()
//    prop.setProperty("user", "root")
//    prop.setProperty("password", "000000")
//
//    trainDF.write.jdbc(s"jdbc:mysql://192.168.0.217:3306/candao", "train_table", prop)
//    evalDF.write.jdbc(s"jdbc:mysql://192.168.0.217:3306/candao", "eval_table", prop)

    //    val pipeline = new Pipeline()
    //    val stages = Array(
    //      new StringIndexer().setInputCol("dayWeather").setOutputCol("dayWeather_index"),
    //      new StringIndexer().setInputCol("nightWeather").setOutputCol("nightWeather_index"),
    //      new StringIndexer().setInputCol("wind").setOutputCol("wind_index"),
    //      new StringIndexer().setInputCol("isWorkday").setOutputCol("isWorkday_index"),
    //      new StringIndexer().setInputCol("isHoliday").setOutputCol("isHoliday_index"),
    //      new VectorAssembler().setInputCols(Array(
    ////        "dayWeather_index",
    ////        "nightWeather_index",
    ////        "wind_index",
    //        "isWorkday_index",
    //        "isHoliday_index",
    //        "sumLast3days",
    //        "meanLast3days",
    //        "sumLast7days",
    //        "meanLast7days",
    //        "sumLast15days",
    //        "meanLast15days"
    //      ))
    //        .setOutputCol("features"),
    //      new GBTRegressor().setFeaturesCol("features").setLabelCol("amount")
    //    )
    //
    //    pipeline.setStages(stages)
    //
    //
    //    val count = df.count()
    //    val model = pipeline.fit(df)
    //
    //    val evaluator = new ExtendedRegressionEvaluator().setLabelCol("amount").setMetricName("mape")
    //    val metric = evaluator.evaluate(model.transform(df))
    //
    //    println(metric)
  }
}
