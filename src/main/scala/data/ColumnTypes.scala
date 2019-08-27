package data

import org.apache.spark.sql.types._

object ColumnTypes {
  def integerType() = IntegerType

  def longType() = LongType

  def floatType() = FloatType

  def doubleType() = DoubleType

  def multiDoubleType() = ArrayType(DoubleType)

  def stringType() = StringType

  def multiStringType() = ArrayType(StringType)

  def multiLongType() = ArrayType(LongType)

  def timestampType() = TimestampType

  def multiTimestampType() = ArrayType(TimestampType)

}
