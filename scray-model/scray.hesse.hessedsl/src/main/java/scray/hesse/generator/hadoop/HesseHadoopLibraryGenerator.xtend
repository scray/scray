package scray.hesse.generator.hadoop

import scray.hesse.generator.HeaderInformation
import scray.hesse.hesseDSL.MaterializedViewStatement

class HesseHadoopLibraryGenerator {
	
	def static getLib(HeaderInformation header, MaterializedViewStatement view) {
		return header.modelname + view.name + "Library"
	}
	
	def static generateLibrary(HeaderInformation header, MaterializedViewStatement view) {
		'''
		// generated using Hesse!
		package «header.modelname»
		
		import com.datastax.driver.core.{ColumnDefinitions, DataType, Row}
		import java.lang.{Integer => JInteger}
		import java.math.{BigInteger => JBigInteger, BigDecimal => JBigDecimal}
		import java.text.SimpleDateFormat
		import java.util.{Calendar, Date}
		import scala.util.Try
		import scala.math.{BigInt, BigDecimal}
		
		object «getLib(header, view)» {
			
			def makeDate(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): Date = {
				val cal = Calendar.getInstance
				cal.set(year, month - 1, day, hour, minute, second)
				cal.getTime
			}
			
			def stringToDate(date: String): Date = {
				val sf = date.substring(0, 11)
				val df1 = new SimpleDateFormat("dd.MM.yyyy")
				val df2 = new SimpleDateFormat("yyyy-MM-dd")
				val df3 = new SimpleDateFormat("MM/dd/yyyy")
				Try(df1.parse(sf)).getOrElse(Try(df2.parse(sf)).getOrElse(df3.parse(sf)))
			}
			
			def dateLib(timestamp: Long): Date = {
				val cal = Calendar.getInstance()
				cal.setTime(new Date(timestamp))	
				cal.set(Calendar.HOUR, 0)
				cal.set(Calendar.MINUTE, 0)
				cal.set(Calendar.SECOND, 0)
				cal.set(Calendar.MILLISECOND, 0)
				cal.getTime()
			}
			
			def extractTime(timestamp: Date, field: Int) = {
				val cal = Calendar.getInstance()
				cal.setTime(timestamp)
				cal.get(field)
			}

			def castToInt(input: Any): Int = Try {
				input match {
					case i: Int => i
					case i: JInteger => i.intValue
					case d: Double => d.toInt
					case l: Long => l.toInt
					case f: Float => f.toInt
					case b: BigInt => b.toInt
					case b: BigDecimal => b.toInt
					case b: JBigInteger => b.intValue
					case b: JBigDecimal => b.intValue
					case s => s.toString.toInt
				}
			}.getOrElse { throw new RuntimeException("TIME: Could not cast input \" + input + \" to Int") }

			def castToDouble(input: Any): Double = Try {
				input match {
					case i: Int => i.toDouble
					case i: JInteger => i.doubleValue
					case d: Double => d
					case l: Long => l.toDouble
					case f: Float => f.toDouble
					case b: BigInt => b.toDouble
					case b: BigDecimal => b.toDouble
					case b: JBigInteger => b.doubleValue
					case b: JBigDecimal => b.doubleValue
					case s => s.toString.toInt
				}
			}.getOrElse { throw new RuntimeException("TIME: Could not cast input \" + input + \" to Double") }
			
			def castToFloat(input: Any): Float = Try {
				input match {
					case i: Int => i.toFloat
					case i: JInteger => i.floatValue
					case d: Double => d.toFloat
					case l: Long => l.toFloat
					case f: Float => f
					case b: BigInt => b.toFloat
					case b: BigDecimal => b.toFloat
					case b: JBigInteger => b.floatValue
					case b: JBigDecimal => b.floatValue
					case s => s.toString.toInt
				}
			}.getOrElse { throw new RuntimeException("TIME: Could not cast input \" + input + \" to Float") }
			
			def castToLong(input: Any): Long = Try {
				input match {
					case i: Int => i.toLong
					case i: JInteger => i.longValue
					case d: Double => d.toLong
					case l: Long => l
					case f: Float => f.toLong
					case b: BigInt => b.toLong
					case b: BigDecimal => b.toLong
					case b: JBigInteger => b.longValue
					case b: JBigDecimal => b.longValue
					case s => s.toString.toInt
				}
			}.getOrElse { throw new RuntimeException("TIME: Could not cast input \" + input + \" to Long") }
			
			def getAnyObject(column: String, typ: DataType.Name, row: Row) = typ match {
			    case DataType.Name.ASCII | DataType.Name.TEXT | DataType.Name.VARCHAR => row.getString(column)
			    case DataType.Name.BIGINT | DataType.Name.COUNTER => row.getLong(column)
			    case DataType.Name.BLOB => row.getBytes(column)
			    case DataType.Name.BOOLEAN => row.getBool(column)
			    case DataType.Name.DECIMAL => row.getDecimal(column)
			    case DataType.Name.DOUBLE => row.getDouble(column)
			    case DataType.Name.INT => row.getInt(column)
			    case DataType.Name.VARINT => row.getVarint(column)
			    case DataType.Name.FLOAT => row.getFloat(column)
			    case DataType.Name.TIMESTAMP => row.getDate(column)
			    case DataType.Name.UUID | DataType.Name.TIMEUUID => row.getUUID(column)
			    case DataType.Name.INET => row.getInet(column)
			}
			
			def getAnyObject(definition: ColumnDefinitions.Definition, row: Row): Any = 
				getAnyObject(definition.getName, definition.getType.getName, row)
			
			def getAnyObject(row: Row, column: String): Any = 
				getAnyObject(column, row.getColumnDefinitions.getType(column).getName, row)
		}
		'''
	}
}