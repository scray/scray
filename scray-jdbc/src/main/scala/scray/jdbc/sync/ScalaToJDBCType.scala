package scray.jdbc.sync

import java.sql.Blob

object ScalaToJDBCType {
  
  trait JDBCTypes[+T] {
    def getType: String
    def getDD: T = {
      "".asInstanceOf[T]
    }
  }
  
  def apply[T: JDBCTypes]: JDBCTypes[T] = implicitly[JDBCTypes[T]]
  
  implicit object StringJDBCType extends JDBCTypes[String] {
    def getType: String = "VARCHAR"
  }
  
  implicit object BooleanJDBCType extends JDBCTypes[Boolean] {
    def getType: String = "BIT"
  }
  
  implicit object IntJDBCType extends JDBCTypes[Int] {
    def getType: String = "INTEGER"
  }
  
  implicit object LongJDBCType extends JDBCTypes[Long] {
    def getType: String = "BIGINT"
  }
  
  implicit object FloatJDBCType extends JDBCTypes[Float] {
    def getType: String = "FLOAT"
  }
  
  implicit object DoubleJDBCType extends JDBCTypes[Double] {
    def getType: String = "DOUBLE"
  }
  
  implicit object BlobJDBCType extends JDBCTypes[Blob] {
    def getType: String = "BLOB"
  }
}