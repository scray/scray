package scray.querying.description

import scala.math.BigDecimal
import java.math.BigInteger
import java.math.{BigDecimal => JBigDecimal}
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
  val BIGDECIMAL = Value(9,  classOf[JBigDecimal].getName)
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

object BigDecimalType extends ColumnType[JBigDecimal] {
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

class StringColumn(override val column: Column, override val value: String) 
  extends RowColumn[String](column, value) with SpecializedColumn[String] {
  override val columnType = StringType
}

class LongColumn(override val column: Column, override val value: Long) 
  extends RowColumn[Long](column, value) with SpecializedColumn[Long] {
  override val columnType = LongType
}

class IntColumn(override val column: Column, override val value: Int) 
  extends RowColumn[Int](column, value) with SpecializedColumn[Int] {
  override val columnType = IntType
}

class UUIDColumn(override val column: Column, override val value: UUID) 
  extends RowColumn[UUID](column, value) with SpecializedColumn[UUID] {
  override val columnType = UUIDType
}

class ShortColumn(override val column: Column, override val value: Short) 
  extends RowColumn[Short](column, value) with SpecializedColumn[Short] {
  override val columnType = ShortType
}

class FloatColumn(override val column: Column, override val value: Float) 
  extends RowColumn[Float](column, value) with SpecializedColumn[Float] {
  override val columnType = FloatType
}

class DoubleColumn(override val column: Column, override val value: Double) 
  extends RowColumn[Double](column, value) with SpecializedColumn[Double] {
  override val columnType = DoubleType
}

class BigIntColumn(override val column: Column, override val value: BigInt) 
  extends RowColumn[BigInt](column, value) with SpecializedColumn[BigInt] {
  override val columnType = BigIntType
}

class BigDecimalColumn(override val column: Column, override val value: JBigDecimal) 
  extends RowColumn[JBigDecimal](column, value) with SpecializedColumn[JBigDecimal] {
  override val columnType = BigDecimalType
}

class BigIntegerColumn(override val column: Column, override val value: BigInteger) 
  extends RowColumn[BigInteger](column, value) with SpecializedColumn[BigInteger] {
  override val columnType = BigIntegerType
}

class IntegerColumn(override val column: Column, override val value: Integer) 
  extends RowColumn[Integer](column, value) with SpecializedColumn[Integer] {
  override val columnType = IntegerType
}

class ObjectColumn(override val column: Column, override val value: Object) 
  extends RowColumn[Object](column, value) with SpecializedColumn[Object] {
  override val columnType = ObjectType
}

class ByteColumn(override val column: Column, override val value: Byte) 
  extends RowColumn[Byte](column, value) with SpecializedColumn[Byte] {
  override val columnType = ByteType
}

class ArrayByteColumn(override val column: Column, override val value: Array[Byte]) 
  extends RowColumn[Array[Byte]](column, value) with SpecializedColumn[Array[Byte]] {
  override val columnType = ByteArrType
}

class DateColumn(override val column: Column, override val value: Date) 
  extends RowColumn[Date](column, value) with SpecializedColumn[Date] {
  override val columnType = DateType
}

class TimeColumn(override val column: Column, override val value: Time) 
  extends RowColumn[Time](column, value) with SpecializedColumn[Time] {
  override val columnType = TimeType
}

class TimestampColumn(override val column: Column, override val value: Timestamp) 
  extends RowColumn[Timestamp](column, value) with SpecializedColumn[Timestamp] {
  override val columnType = TimestampType
}

class URLColumn(override val column: Column, override val value: URL) 
  extends RowColumn[URL](column, value) with SpecializedColumn[URL] {
  override val columnType = URLType
}

class URIColumn(override val column: Column, override val value: URI) 
  extends RowColumn[URI](column, value) with SpecializedColumn[URI] {
  override val columnType = URIType
}


object ColumnFactory {
  
  def getColumnFromValue(col:Column, value: Any): RowColumn[_] = {
    value match {
      case i: Int => new IntColumn(col, i)
      case s: String => new StringColumn(col, s)
      case bi: BigInt => new BigIntegerColumn(col, bi.bigInteger)
      case bi: BigInteger => new BigIntegerColumn(col, bi)
      case bc: BigDecimal => new BigDecimalColumn(col, bc.bigDecimal)
      case jbc: JBigDecimal => new BigDecimalColumn(col, jbc)
      case u: URL => new URLColumn(col, new URL(u.toString()))
      case u: URI => new URIColumn(col, new URI(u.toString()))
      case t: Timestamp => new TimestampColumn(col, new Timestamp(t.getTime()))
      case t: Time => new TimeColumn(col, new Time(t.getTime()))
      case d: Date => new DateColumn(col, d)
      case b: Byte => new ByteColumn(col, b)
      case f: Float => new FloatColumn(col, f)
      case db: Double => new DoubleColumn(col, db)
      case a: Array[Byte] => new ArrayByteColumn(col, a)
      case _ => new ObjectColumn(col, value.asInstanceOf[Object]).asInstanceOf[RowColumn[Object]]
    }
  }
  
}

