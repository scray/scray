package scray.querying.description

import java.math.BigInteger
import java.math.BigDecimal
import java.util.UUID
import java.net.URI
import java.net.URL
import java.sql.Timestamp
import java.sql.Date
import java.sql.Time

object BasicTypeIds extends Enumeration {
  type BasicTypeIds = Value
  val STRING     = Value(1,  classOf[String].getName)
  val LONG       = Value(2,  classOf[Long].getName)
  val INT        = Value(3,  classOf[Int].getName)
  val UUID       = Value(4,  classOf[UUID].getName)
  val SHORT      = Value(5,  classOf[Short].getName)
  val FLOAT      = Value(6,  classOf[Float].getName)
  val DOUBLE     = Value(7,  classOf[Double].getName)
  val BIGINT     = Value(8,  classOf[BigInt].getName)
  val BIGDECIMAL = Value(9,  classOf[BigDecimal].getName)
  val BIGINTEGER = Value(10, classOf[BigInteger].getName)
  val INTEGER    = Value(11, classOf[Integer].getName)
  val OBJECT     = Value(13, classOf[Object].getName)
  val BYTE       = Value(14, classOf[Byte].getName)
  val BYTEARR    = Value(15, classOf[Array[Byte]].getName)
  val DATE       = Value(16, classOf[Date].getName)
  val TIME       = Value(17, classOf[Time].getName)
  val TIMESTAMP  = Value(18, classOf[Timestamp].getName)
  val URL        = Value(19, classOf[URL].getName)
  val URI        = Value(20, classOf[URI].getName)
}

trait ColumnType[T] extends Serializable {
  type ColumnType = T
  val typeId: Int
  val className: String
  def isPrimitiveType: Boolean = false
}

object StringType extends ColumnType[String] {
  override val typeId: Int = BasicTypeIds.STRING.id
  override val className: String = BasicTypeIds.STRING.toString
}

object LongType extends ColumnType[Long] {
  override val typeId: Int = BasicTypeIds.LONG.id
  override val className: String = BasicTypeIds.LONG.toString
}

object ShortType extends ColumnType[Short] {
  override val typeId: Int = BasicTypeIds.SHORT.id
  override val className: String = BasicTypeIds.SHORT.toString
}

object DoubleType extends ColumnType[Double] {
  override val typeId: Int = BasicTypeIds.DOUBLE.id
  override val className: String = BasicTypeIds.DOUBLE.toString
}

object FloatType extends ColumnType[Float] {
  override val typeId: Int = BasicTypeIds.FLOAT.id
  override val className: String = BasicTypeIds.FLOAT.toString
}

object IntType extends ColumnType[Int] {
  override val typeId: Int = BasicTypeIds.INT.id
  override val className: String = BasicTypeIds.INT.toString
}

object UUIDType extends ColumnType[UUID] {
  override val typeId: Int = BasicTypeIds.UUID.id
  override val className: String = BasicTypeIds.UUID.toString
}

object BigIntType extends ColumnType[BigInt] {
  override val typeId: Int = BasicTypeIds.BIGINT.id
  override val className: String = BasicTypeIds.BIGINT.toString
}

object BigDecimalType extends ColumnType[BigDecimal] {
  override val typeId: Int = BasicTypeIds.BIGDECIMAL.id
  override val className: String = BasicTypeIds.BIGDECIMAL.toString
}

object BigIntegerType extends ColumnType[BigInteger] {
  override val typeId: Int = BasicTypeIds.BIGINTEGER.id
  override val className: String = BasicTypeIds.BIGINTEGER.toString
}

object IntegerType extends ColumnType[Integer] {
  override val typeId: Int = BasicTypeIds.INTEGER.id
  override val className: String = BasicTypeIds.INTEGER.toString
}

object ObjectType extends ColumnType[Object] {
  override val typeId: Int = BasicTypeIds.OBJECT.id
  override val className: String = BasicTypeIds.OBJECT.toString
}

object ByteType extends ColumnType[Byte] {
  override val typeId: Int = BasicTypeIds.BYTE.id
  override val className: String = BasicTypeIds.BYTE.toString
}

object ByteArrType extends ColumnType[Array[Byte]] {
  override val typeId: Int = BasicTypeIds.BYTEARR.id
  override val className: String = BasicTypeIds.BYTEARR.toString
}

object DateType extends ColumnType[Date] {
  override val typeId: Int = BasicTypeIds.DATE.id
  override val className: String = BasicTypeIds.DATE.toString
}

object TimeType extends ColumnType[Time] {
  override val typeId: Int = BasicTypeIds.TIME.id
  override val className: String = BasicTypeIds.TIME.toString
}

object TimestampType extends ColumnType[Timestamp] {
  override val typeId: Int = BasicTypeIds.TIMESTAMP.id
  override val className: String = BasicTypeIds.TIMESTAMP.toString
}

object URLType extends ColumnType[URL] {
  override val typeId: Int = BasicTypeIds.URL.id
  override val className: String = BasicTypeIds.URL.toString
}

object URIType extends ColumnType[URI] {
  override val typeId: Int = BasicTypeIds.URI.id
  override val className: String = BasicTypeIds.URI.toString
}



trait SpecializedColumn[T] extends Serializable {
  val columnType: ColumnType[T]
  
  
}