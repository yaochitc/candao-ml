import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Candao2 {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 5)
    {
      val dish = "dish" + i
      val dataHose = CandaoUtil.getCandaoByDish(dish)
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
          Integer.parseInt(colContent.replaceAll("℃", "")).toDouble
      }, DoubleType)

      df = df.withColumn("date", parseDate(col("date")))
        .withColumn("dayTemp", parseTemp(col("dayTemp")))
        .withColumn("nightTemp", parseTemp(col("nightTemp")))

      val monthOfYear = udf({
        timestamp: Timestamp =>
          timestamp.getMonth.toDouble
      }, DoubleType)

      val dayOfWeek = udf({
        timestamp: Timestamp =>
          timestamp.getDay.toDouble
      }, DoubleType)

      df = df.withColumn("monthOfYear", monthOfYear(col("date")))
        .withColumn("dayOfWeek", dayOfWeek(col("date")))

      val getDate = udf({
        window: GenericRowWithSchema =>
          val end = window.getTimestamp(1)
          new Timestamp(end.getTime + 24 * 3600 * 1000)
      }, TimestampType)

      val df3Days = df.groupBy(window(col("date"), "3 days", "1 day"))
        .agg(
          expr(s"sum(amount) as sumLast3days"),
          expr(s"mean(amount) as meanLast3days")
        ).withColumn("date", getDate(col("window")))
        .select("date", "sumLast3days", "meanLast3days")

      val df7Days = df.groupBy(window(col("date"), "7 days", "1 day"))
        .agg(
          expr(s"sum(amount) as sumLast7days"),
          expr(s"mean(amount) as meanLast7days")
        ).withColumn("date", getDate(col("window")))
        .select("date", "sumLast7days", "meanLast7days")

      val df15Days = df.groupBy(window(col("date"), "15 days", "1 day"))
        .agg(
          expr(s"sum(amount) as sumLast15days"),
          expr(s"mean(amount) as meanLast15days")
        ).withColumn("date", getDate(col("window")))
        .select("date", "sumLast15days", "meanLast15days")

      df = df.join(df3Days, Array("date"))
        .join(df7Days, Array("date"))
        .join(df15Days, Array("date"))

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


      df = df.select(
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

      val prop = new Properties()
      prop.setProperty("user", "root")
      prop.setProperty("password", "000000")

      trainDF.write.jdbc(s"jdbc:mysql://192.168.0.217:3306/candao", String.format("train_table_%s", dish), prop)
      evalDF.write.jdbc(s"jdbc:mysql://192.168.0.217:3306/candao", String.format("eval_table_%s", dish), prop)
    }
  }
}
