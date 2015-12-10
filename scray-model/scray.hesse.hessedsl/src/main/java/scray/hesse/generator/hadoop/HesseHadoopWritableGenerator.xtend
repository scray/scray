package scray.hesse.generator.hadoop

import java.util.ArrayList
import java.util.List
import java.util.concurrent.atomic.AtomicInteger
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.generator.StaticUtilities
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.GroupingDefinitions
import scray.hesse.hesseDSL.MaterializedViewStatement
import scray.hesse.hesseDSL.SelectColumn
import scray.hesse.hesseDSL.SupportedDBMSSystems

/**
 * writes temporary information 
 */
class HesseHadoopWritableGenerator {
	
	/**
	 * identify columns we need to use in the key (and the order) to transfer to the reducer and sort
	 * TODO: resolve variables and replace with these
	 */
	def List<String> getKeyColumnNames(MaterializedViewStatement view) {
		// deciding about the mapper output key structure:
		// 1. if there is grouping -> we need to group by the given column names 
		// 1a. if there is no additional ordering -> make keys the columns to be grouped
		// 1b. if there is additional ordering 
		// 1b1. we need to order in another hadoop job, if the target store is not automatically sorting, but we return 
		// 1b2. we need to do nothing else if the order by columns are equivalent or an equally ordered subset of the group by columns
		// 1b3. 
		// 2. if there is ordering without grouping -> we can use the columns provided in the order by clause
		// 3. if there is neither grouping nor ordering we are in luck and can use a random/increasing value
		if(StaticUtilities::isSet(view.select, "groupings")) {
			val GroupingDefinitions groupedColumns = view.getSelect().getGroupings()
			// 1.
			if(!StaticUtilities::isSet(view.select, "orderings")) {
				// 1a.
				groupedColumns.getColumns().map [ SelectColumn selectColumn | selectColumn.name ]
			} else {
				val orderedColumns = view.getSelect().getOrderings()
				// 1b.
				// check that the content and order of orderings is smaller or identical 
				// to groupings but there are not more orderings than groupings
				// TODO: check tables
				//for(col: orderedColumns) {
					// actually this might not be done in here, as we need to return groupings anyway 
				//}
				groupedColumns.columns.map [ SelectColumn selectColumn | selectColumn.name ]
			}
		} else if (StaticUtilities::isSet(view.select, "orderings")) {
			// 2.
			view.getSelect().getOrderings().getColumns().map [ SelectColumn selectColumn | selectColumn.name ]
		} else {
			// 3.
			new ArrayList<String>()
		}
	}

	/**
	 * Creates a Writable for internal use, that is used to transfer all values from the mappers
	 */
	private def generateRowWritable(HeaderInformation header, MaterializedViewStatement view) {
		'''
		/**
		 * communicate and store complete row data in custom Hadoop format
		 */
		class «header.modelname + view.name»RowBytesWritable extends Writable {
			import com.twitter.chill._
			import java.io.{DataInput, DataOutput}
			
			val kryo = ScalaKryoInstantiator.defaultPool
			
			var rowopt: Option[Row] = None
			
			this(row: Row) = {
				this()
				rowopt = Some(row)
			}
			
			override def write(out: DataOutput): Unit = {
				«IF header.isDBMSUsed(#[view], SupportedDBMSSystems.CASSANDRA)»
					val map = rowopt.map(row => row.getColumnDefinitions.iterator.map { definition => definition.name -> row.getObject(definition.name) }.toMap[String, Any]).getOrElse(Map[String, Any]())
				«ELSEIF header.isDBMSUsed(#[view], SupportedDBMSSystems.MYSQL) || header.isDBMSUsed(#[view], SupportedDBMSSystems.ORACLE)»
					val map = rowopt.map(row => row.asInstanceOf[«header.modelname + view.name»CQLCassandraRow].columns).getOrElse(Map[String, Any]())
				«ENDIF»
				out.writeInt(map.size)
				map.foreach { entry =>
					val (key, value) = entry
					out.writeUTF(key)
					val valuebytes = kryo.toBytesWithClass(key)
					out.writeInt(valuebytes.length)
					out.write(valuebytes)
				}
			}
			override def readFields(in: DataInput): Unit = {
				val numberOfEntries = in.readInt
				val resultMap = new LinkedHashMap[String, Any]
				(1 to numberOfEntries).foreach { _ =>
					val key = in.readUTF
					val bytes = new Array[Byte](in.readInt)
					val value = kryo.fromBytes(bytes)
					resultMap += ((key, value))
				}
				new «header.modelname + view.name»CQLCassandraRow(resultMap)
			}
		}
		
		object «header.modelname + view.name»RowBytesWritable {
			import java.io.DataInput
			
			def read(in: DataInput): «header.modelname + view.name»RowBytesWritable = {
				val result = new «header.modelname + view.name»RowBytesWritable()
				result.readFields(in)
				result
			}
		}
		'''
	}
	
	/**
	 * generate the Writable used to transfer and order the keys
	 */
	def generateKeyWritable(List<String> variables, HeaderInformation header, MaterializedViewStatement view) {
		generateWritable(variables, "Key", header, view)		
	}
	
	/**
	 * generate the Writable used to transfer values, i.e. Rows
	 */
	def generateValueWritable(HeaderInformation header, MaterializedViewStatement view) {
		generateRowWritable(header, view)
	}
	
	/**
	 * helper method to generically generate an OutputWritable
	 */
	private def generateWritable(List<String> variables, String prefix, HeaderInformation header, MaterializedViewStatement view) {
		val varbuffer = new StringBuffer
		variables.forEach [ name |
			varbuffer.append("var ")
			varbuffer.append(name)
			varbuffer.append(": Any = null")
			varbuffer.append(System::lineSeparator)
		]
		val count = new AtomicInteger(0)
		val writebuffer = new StringBuffer
		variables.forEach [ name |
			writebuffer.append("val bytes")
			writebuffer.append(count.get)
			writebuffer.append(" = kryo.toBytesWithClass(")
			writebuffer.append(name)
			writebuffer.append(")")
			writebuffer.append(System::lineSeparator)
			writebuffer.append("out.writeInt(bytes")
			writebuffer.append(count.get)
			writebuffer.append(".length)")
			writebuffer.append(System::lineSeparator)
			writebuffer.append("out.write(bytes")
			writebuffer.append(count.get)
			writebuffer.append(")")
			writebuffer.append(System::lineSeparator)
			count.incrementAndGet
		]
		count.set(0)
		val readbuffer = new StringBuffer
		variables.forEach [ name |
			readbuffer.append("val bytes")
			readbuffer.append(count.get)
			readbuffer.append(" = new Array[Byte](in.readInt)")
			readbuffer.append(System::lineSeparator)
			readbuffer.append("in.readFully(bytes")
			readbuffer.append(count.get)
			readbuffer.append(")")
			readbuffer.append(System::lineSeparator)
			readbuffer.append(name)
			readbuffer.append(" = kryo.fromBytes(bytes")
			readbuffer.append(count.get)
			readbuffer.append(")")
			readbuffer.append(System::lineSeparator)
			count.incrementAndGet
		]
		'''
		/**
		 * communicate and store complete row data in custom Hadoop format for «prefix»
		 */
		class «header.modelname + view.name + prefix»BytesWritable extends Writable {
			import com.twitter.chill._
			import java.io.{DataInput, DataOutput}
			
			val kryo = ScalaKryoInstantiator.defaultPool
			
			«varbuffer.toString»
			
			override def write(out: DataOutput): Unit = {
				var count = 0
				«writebuffer.toString»
			}
			override def readFields(in: DataInput): Unit = {
				«readbuffer.toString»
			}
		}
		
		object «header.modelname + view.name + prefix»BytesWritable {
			import java.io.DataInput
			
			def read(in: DataInput): «header.modelname + view.name + prefix»BytesWritable = {
				val result = new «header.modelname + view.name + prefix»BytesWritable()
				result.readFields(in)
				result
			}
		}
		'''
	}
	
	/**
	 * creates an internal row representation to work with
	 */
	def generateTemporaryRow(HeaderInformation header, MaterializedViewStatement view) {
		'''
		/**
		 * a simple cassandra row implementation which we also map data from all other stores to
		 * to get a single accessor to our data.
		 */
		class «header.modelname + view.name»CQLCassandraRow(columns: LinkedHashMap[String, _]) extends Row {
			import scala.collection.JavaConverters._
			private def get[T](name: String) = columns.get(name).get.asInstanceOf[T]
			override def getColumnDefinitions(): ColumnDefinitions = ???
			override def isNull(i: Int): Boolean = ???
			override def isNull(name: String): Boolean = columns.get(name).isEmpty
			override def getBool(i: Int): Boolean = ???
			override def getBool(name: String): Boolean = get[Boolean](name)
			override def getInt(i: Int): Int = ???
			override def getInt(name: String): Int = get[Int](name)
			override def getLong(i: Int): Long = ???
			override def getLong(name: String): Long = get[Long](name)
			override def getDate(i: Int): Date = ???
			override def getDate(name: String): Date = get[Date](name)
			override def getFloat(i: Int): Float = ???
			override def getFloat(name: String): Float = get[Float](name)
			override def getDouble(i: Int): Double = ???
			override def getDouble(name: String): Double = get[Double](name)
			override def getBytesUnsafe(i: Int): ByteBuffer = ???
			override def getBytesUnsafe(name: String): ByteBuffer = ByteBuffer.wrap(get[Array[Byte]](name))
			override def getBytes(i: Int): ByteBuffer = ???
			override def getBytes(name: String): ByteBuffer = ByteBuffer.wrap(get[Array[Byte]](name))
			override def getString(i: Int): String = ???
			override def getString(name: String): String = get[String](name)
			override def getVarint(i: Int): JBigInteger = ???
			override def getVarint(name: String): JBigInteger = get[JBigInteger](name)
			override def getDecimal(i: Int): JBigDecimal = ???
			override def getDecimal(name: String): JBigDecimal = get[JBigDecimal](name)
			override def getUUID(i: Int): UUID = ???
			override def getUUID(name: String): UUID = get[UUID](name)
			override def getInet(i: Int): InetAddress = ???
			override def getInet(name: String): InetAddress = get[InetAddress](name)
			override def getList[T](i: Int, elementsClass: Class[T]): JList[T] = ???
			override def getList[T](name: String, elementsClass: Class[T]): JList[T] = get[List[T]](name).asJava
			override def getSet[T](i: Int, elementsClass: Class[T]): JSet[T] = ???
			override def getSet[T](name: String, elementsClass: Class[T]): JSet[T] = get[Set[T]](name).asJava
			override def getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V]): JMap[K, V] = ???
			override def getMap[K, V](name: String, keysClass: Class[K], valuesClass: Class[V]): JMap[K, V] = get[Map[K, V]](name).asJava
			override def getTupleValue(i: Int): TupleValue = ???
			override def getTupleValue(name: String): TupleValue = get[TupleValue](name)
			override def getUDTValue(i: Int): UDTValue = ???
			override def getUDTValue(name: String): UDTValue = get[UDTValue](name)
			override def getList[T](i: Int, tt: TypeToken[T]): JList[T] = ???
			override def getMap[K, V](i: Int, tt1: TypeToken[K], tt2: TypeToken[V]): JMap[K,V] = ???
			override def getSet[T](i: Int, tt: TypeToken[T]): JSet[T] = ???
			override def getList[T](name: String, tt: TypeToken[T]): JList[T] = get[List[T]](name).asJava
			override def getMap[K, V](name: String, tt1: TypeToken[K], tt2: TypeToken[V]): JMap[K,V] = get[Map[K, V]](name).asJava
			override def getSet[T](name: String, tt: TypeToken[T]): JSet[T] = get[Set[T]](name).asJava
			override def getPartitionKeyToken(): Token = ???
			override def getToken(name: String): Token = get[Token](name)
		  	override def getToken(i: Int): Token = ???
		  	override def getObject(name: String): Object = get[Object](name)
		 	override def getObject(i: Int): Object = ???
		}
		'''
	}
	
	/**
	 * generates a database InputWritable
	 */
	def generateInputDBWritable(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		'''
		«IF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.MYSQL) || header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.ORACLE)»			
		/**
		 * Using DBInputFormat requires us to use a DBWritable to carry input data.
		 * Reads all data from the given table.
		 */
		class «header.modelname + view.name»DBInputWritableRow extends Writable with DBWritable {
			
			var row: Option[Row] = None
			
			// dummy methods
			override readFields(in: DataInput): Unit = {}
			override write(out: DataOutput): Unit = {}
			
			// reads a set from db
			override readFields(rs: ResultSet): Unit = {
				val meta = rs.getMetaData
				val colCount = meta.getColumnCount
				val columns = (1 to colCount).map { colNumber =>
					(meta.getColumnName(colNumber,
					meta.getColumnType(colNumber) match {
						case Types.BIGINT => rs.getLong(colNumber)
						case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR => rs.getString(colNumber)
						case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => rs.getBytes(colNumber)
						case Types.BIT => rs.getBoolean(colNumber)
						case Types.NUMERIC | Types.DECIMAL => rs.getBigDecimal(colNumber)
						case Types.REAL => rs.getFloat(colNumber)
						case Types.FLOAT | Types.DOUBLE => rs.getDouble(colNumber)
						case Types.DATE | Types.TIMESTAMP | Types.TIME => rs.getTimestamp(colNumber)
						case Types.TINYINT => rs.getByte(colNumber).toInt
						case Types.SMALLINT => rs.getShort(colNumber).toInt
						case Types.INTEGER => rs.getInt(colNumber)
					})
				}
				val columnMap = new LinkedHashMap[]
				columnMap ++= columns.toMap
				row = Some(new «header.modelname + view.name»CQLCassandraRow(columnMap))
			}
			
			// writes a set to db
			override write(ps: PreparedStatement): Unit = {}
			
			def getRow(): Option[Row] = row
		}
		«ENDIF»
		'''
	}
	
}