package scray.querying.sync.oracle

//object OracleImplementation {
//  val typeMapping : Map[String, Class[_]] = Map[String, Class[_]] (
//    "BIT" -> classOf[Boolean],
//    "TINYINT" -> classOf[Byte],
//    "SMALLINT" -> classOf[Short],
//    "INTEGER" -> classOf[Int],
//    "BIGINT" -> classOf[BigInteger],
//    "REAL" -> classOf[Float],
//    "DOUBLE" -> classOf[Double],
//    "VARCHAR" -> classOf[String],
//    "TIME" -> classOf[Time],
//    "TIMESTAMP" -> classOf[Timestamp],
//    "VARBINARY" -> classOf[Array[Byte]],
//    "STRUCT" -> classOf[Struct],
//    "BLOB" -> classOf[Blob],
//    "REF" -> classOf[Ref],
//    "CLOB" -> classOf[Clob],
//    "ROWID" -> classOf[RowId],
//    "DATALINK" -> classOf[URL],
//    "SQLXML" -> classOf[SQLXML],
//    "NCLOB" -> classOf[NClob]
//  )
//
//  val allInstances = true :: 1 :: 1L :: HNil
//  
//  val changedMap: Map[Class[_], String] = typeMapping.map(x => (x._2, x._1)).toMap
//
//  private trait ResultInternalRecursionImplicits[T, S <: HList] {
//    def apply(list: S): DBColumnImplementation[T]
//  }
//  
//  private object InternalRecursionImplicits {
//  
//    implicit def hnilCreate[U]: ResultInternalRecursionImplicits[U, HNil] = new ResultInternalRecursionImplicits[U, HNil] {
//      override def apply(list: HNil): DBColumnImplementation[U] = null
//    }
//    
//    implicit def hlistInductionCreate[M, N <: HList](implicit a2c: ResultInternalRecursionImplicits[M, N]): ResultInternalRecursionImplicits[M, M :: N] = 
//        new ResultInternalRecursionImplicits[M, M :: N] {
//          def apply(list: M :: N): DBColumnImplementation[M] = {
//            changedMap.get(list.head.getClass).map { str => new DBColumnImplementation[M] { override def getDBType = str }}.getOrElse(a2c(list.tail))
//          }
//        }
//  }
//
//  implicit def genericOracleColumnImplicit[T](implicit a2c: ResultInternalRecursionImplicits[T, N]): DBColumnImplementation[T] = 
//}