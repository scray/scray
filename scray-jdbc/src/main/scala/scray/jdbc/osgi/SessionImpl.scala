package scray.jdbc.osgi

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLWarning
import java.sql.Statement

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.scalalogging.LazyLogging

import scray.jdbc.sync.JDBCDbSession
import scray.jdbc.sync.JDBCDbSessionImpl
import scray.querying.sync2.DbSession
import slick.dbio.DBIOAction
import slick.jdbc.JdbcProfile
import slick.sql.FixedSqlAction


class SessionImpl(s: JDBCDbSessionImpl) extends JDBCDbSession with LazyLogging with java.sql.ResultSet with DbSession[PreparedStatement, PreparedStatement, ResultSet, JdbcProfile] {
 var res: Option[ResultSet] = None
 
  def executeHive(q: String): Unit = {
   res = s.executeQuery(q) match {
     case Failure(e) => {
       logger.error(s"Querry error: ${e}")
       None
     }
     case Success(r) => Some(r)
   }
  }

  def absolute(x$1: Int): Boolean = ???
  def afterLast(): Unit = ???
  def beforeFirst(): Unit = ???
  def cancelRowUpdates(): Unit = ???
  def clearWarnings(): Unit = ???
  def close(): Unit = ???
  def deleteRow(): Unit = ???
  def findColumn(x$1: String): Int = ???
  def first(): Boolean = ???
  def getArray(x$1: String): java.sql.Array = ???
  def getArray(x$1: Int): java.sql.Array = ???
  def getAsciiStream(x$1: String): java.io.InputStream = ???
  def getAsciiStream(x$1: Int): java.io.InputStream = ???
  def getBigDecimal(x$1: String): java.math.BigDecimal = ???
  def getBigDecimal(x$1: Int): java.math.BigDecimal = ???
  def getBigDecimal(x$1: String,x$2: Int): java.math.BigDecimal = ???
  def getBigDecimal(x$1: Int,x$2: Int): java.math.BigDecimal = ???
  def getBinaryStream(x$1: String): java.io.InputStream = ???
  def getBinaryStream(x$1: Int): java.io.InputStream = ???
  def getBlob(x$1: String): java.sql.Blob = ???
  def getBlob(x$1: Int): java.sql.Blob = ???
  def getBoolean(x$1: String): Boolean = ???
  def getBoolean(x$1: Int): Boolean = ???
  def getByte(x$1: String): Byte = ???
  def getByte(x$1: Int): Byte = ???
  def getBytes(x$1: String): Array[Byte] = ???
  def getBytes(x$1: Int): Array[Byte] = ???
  def getCharacterStream(x$1: String): java.io.Reader = ???
  def getCharacterStream(x$1: Int): java.io.Reader = ???
  def getClob(x$1: String): java.sql.Clob = ???
  def getClob(x$1: Int): java.sql.Clob = ???
  def getConcurrency(): Int = ???
  def getCursorName(): String = ???
  def getDate(x$1: String,x$2: java.util.Calendar): java.sql.Date = ???
  def getDate(x$1: Int,x$2: java.util.Calendar): java.sql.Date = ???
  def getDate(x$1: String): java.sql.Date = ???
  def getDate(x$1: Int): java.sql.Date = ???
  def getDouble(x$1: String): Double = ???
  def getDouble(x$1: Int): Double = ???
  def getFetchDirection(): Int = ???
  def getFetchSize(): Int = ???
  def getFloat(x$1: String): Float = ???
  def getFloat(x$1: Int): Float = ???
  def getHoldability(): Int = ???
  def getInt(x$1: String): Int = {
    res.get.getInt(x$1)
  }
  def getInt(x$1: Int): Int = {
    res.get.getInt(x$1)
  }
  def getLong(x$1: String): Long = ???
  def getLong(x$1: Int): Long = ???
  def getMetaData(): java.sql.ResultSetMetaData = ???
  def getNCharacterStream(x$1: String): java.io.Reader = ???
  def getNCharacterStream(x$1: Int): java.io.Reader = ???
  def getNClob(x$1: String): java.sql.NClob = ???
  def getNClob(x$1: Int): java.sql.NClob = ???
  def getNString(x$1: String): String = ???
  def getNString(x$1: Int): String = ???
  def getObject[T](x$1: String,x$2: Class[T]): T = ???
  def getObject[T](x$1: Int,x$2: Class[T]): T = ???
  def getObject(x$1: String,x$2: java.util.Map[String,Class[_]]): Object = ???
  def getObject(x$1: Int,x$2: java.util.Map[String,Class[_]]): Object = ???
  def getObject(x$1: String): Object = ???
  def getObject(x$1: Int): Object = ???
  def getRef(x$1: String): java.sql.Ref = ???
  def getRef(x$1: Int): java.sql.Ref = ???
  def getRow(): Int = ???
  def getRowId(x$1: String): java.sql.RowId = ???
  def getRowId(x$1: Int): java.sql.RowId = ???
  def getSQLXML(x$1: String): java.sql.SQLXML = ???
  def getSQLXML(x$1: Int): java.sql.SQLXML = ???
  def getShort(x$1: String): Short = ???
  def getShort(x$1: Int): Short = ???
  def getStatement(): java.sql.Statement = ???
  def getString(x$1: String): String = {
    res.get.getString(x$1)
  }
  def getString(x$1: Int): String = {
    res.get.getString(x$1)
  }
  def getTime(x$1: String,x$2: java.util.Calendar): java.sql.Time = ???
  def getTime(x$1: Int,x$2: java.util.Calendar): java.sql.Time = ???
  def getTime(x$1: String): java.sql.Time = ???
  def getTime(x$1: Int): java.sql.Time = ???
  def getTimestamp(x$1: String,x$2: java.util.Calendar): java.sql.Timestamp = ???
  def getTimestamp(x$1: Int,x$2: java.util.Calendar): java.sql.Timestamp = ???
  def getTimestamp(x$1: String): java.sql.Timestamp = ???
  def getTimestamp(x$1: Int): java.sql.Timestamp = ???
  def getType(): Int = ???
  def getURL(x$1: String): java.net.URL = ???
  def getURL(x$1: Int): java.net.URL = ???
  def getUnicodeStream(x$1: String): java.io.InputStream = ???
  def getUnicodeStream(x$1: Int): java.io.InputStream = ???
  def getWarnings(): java.sql.SQLWarning = ???
  def insertRow(): Unit = ???
  def isAfterLast(): Boolean = ???
  def isBeforeFirst(): Boolean = ???
  def isClosed(): Boolean = ???
  def isFirst(): Boolean = ???
  def isLast(): Boolean = ???
  def last(): Boolean = ???
  def moveToCurrentRow(): Unit = ???
  def moveToInsertRow(): Unit = ???
  def next(): Boolean = {
    res.get.next()
  }
  def previous(): Boolean = ???
  def refreshRow(): Unit = ???
  def relative(x$1: Int): Boolean = ???
  def rowDeleted(): Boolean = ???
  def rowInserted(): Boolean = ???
  def rowUpdated(): Boolean = ???
  def setFetchDirection(x$1: Int): Unit = ???
  def setFetchSize(x$1: Int): Unit = ???
  def updateArray(x$1: String,x$2: java.sql.Array): Unit = ???
  def updateArray(x$1: Int,x$2: java.sql.Array): Unit = ???
  def updateAsciiStream(x$1: String,x$2: java.io.InputStream): Unit = ???
  def updateAsciiStream(x$1: Int,x$2: java.io.InputStream): Unit = ???
  def updateAsciiStream(x$1: String,x$2: java.io.InputStream,x$3: Long): Unit = ???
  def updateAsciiStream(x$1: Int,x$2: java.io.InputStream,x$3: Long): Unit = ???
  def updateAsciiStream(x$1: String,x$2: java.io.InputStream,x$3: Int): Unit = ???
  def updateAsciiStream(x$1: Int,x$2: java.io.InputStream,x$3: Int): Unit = ???
  def updateBigDecimal(x$1: String,x$2: java.math.BigDecimal): Unit = ???
  def updateBigDecimal(x$1: Int,x$2: java.math.BigDecimal): Unit = ???
  def updateBinaryStream(x$1: String,x$2: java.io.InputStream): Unit = ???
  def updateBinaryStream(x$1: Int,x$2: java.io.InputStream): Unit = ???
  def updateBinaryStream(x$1: String,x$2: java.io.InputStream,x$3: Long): Unit = ???
  def updateBinaryStream(x$1: Int,x$2: java.io.InputStream,x$3: Long): Unit = ???
  def updateBinaryStream(x$1: String,x$2: java.io.InputStream,x$3: Int): Unit = ???
  def updateBinaryStream(x$1: Int,x$2: java.io.InputStream,x$3: Int): Unit = ???
  def updateBlob(x$1: String,x$2: java.io.InputStream): Unit = ???
  def updateBlob(x$1: Int,x$2: java.io.InputStream): Unit = ???
  def updateBlob(x$1: String,x$2: java.io.InputStream,x$3: Long): Unit = ???
  def updateBlob(x$1: Int,x$2: java.io.InputStream,x$3: Long): Unit = ???
  def updateBlob(x$1: String,x$2: java.sql.Blob): Unit = ???
  def updateBlob(x$1: Int,x$2: java.sql.Blob): Unit = ???
  def updateBoolean(x$1: String,x$2: Boolean): Unit = ???
  def updateBoolean(x$1: Int,x$2: Boolean): Unit = ???
  def updateByte(x$1: String,x$2: Byte): Unit = ???
  def updateByte(x$1: Int,x$2: Byte): Unit = ???
  def updateBytes(x$1: String,x$2: Array[Byte]): Unit = ???
  def updateBytes(x$1: Int,x$2: Array[Byte]): Unit = ???
  def updateCharacterStream(x$1: String,x$2: java.io.Reader): Unit = ???
  def updateCharacterStream(x$1: Int,x$2: java.io.Reader): Unit = ???
  def updateCharacterStream(x$1: String,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateCharacterStream(x$1: Int,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateCharacterStream(x$1: String,x$2: java.io.Reader,x$3: Int): Unit = ???
  def updateCharacterStream(x$1: Int,x$2: java.io.Reader,x$3: Int): Unit = ???
  def updateClob(x$1: String,x$2: java.io.Reader): Unit = ???
  def updateClob(x$1: Int,x$2: java.io.Reader): Unit = ???
  def updateClob(x$1: String,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateClob(x$1: Int,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateClob(x$1: String,x$2: java.sql.Clob): Unit = ???
  def updateClob(x$1: Int,x$2: java.sql.Clob): Unit = ???
  def updateDate(x$1: String,x$2: java.sql.Date): Unit = ???
  def updateDate(x$1: Int,x$2: java.sql.Date): Unit = ???
  def updateDouble(x$1: String,x$2: Double): Unit = ???
  def updateDouble(x$1: Int,x$2: Double): Unit = ???
  def updateFloat(x$1: String,x$2: Float): Unit = ???
  def updateFloat(x$1: Int,x$2: Float): Unit = ???
  def updateInt(x$1: String,x$2: Int): Unit = ???
  def updateInt(x$1: Int,x$2: Int): Unit = ???
  def updateLong(x$1: String,x$2: Long): Unit = ???
  def updateLong(x$1: Int,x$2: Long): Unit = ???
  def updateNCharacterStream(x$1: String,x$2: java.io.Reader): Unit = ???
  def updateNCharacterStream(x$1: Int,x$2: java.io.Reader): Unit = ???
  def updateNCharacterStream(x$1: String,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateNCharacterStream(x$1: Int,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateNClob(x$1: String,x$2: java.io.Reader): Unit = ???
  def updateNClob(x$1: Int,x$2: java.io.Reader): Unit = ???
  def updateNClob(x$1: String,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateNClob(x$1: Int,x$2: java.io.Reader,x$3: Long): Unit = ???
  def updateNClob(x$1: String,x$2: java.sql.NClob): Unit = ???
  def updateNClob(x$1: Int,x$2: java.sql.NClob): Unit = ???
  def updateNString(x$1: String,x$2: String): Unit = ???
  def updateNString(x$1: Int,x$2: String): Unit = ???
  def updateNull(x$1: String): Unit = ???
  def updateNull(x$1: Int): Unit = ???
  def updateObject(x$1: String,x$2: Any): Unit = ???
  def updateObject(x$1: String,x$2: Any,x$3: Int): Unit = ???
  def updateObject(x$1: Int,x$2: Any): Unit = ???
  def updateObject(x$1: Int,x$2: Any,x$3: Int): Unit = ???
  def updateRef(x$1: String,x$2: java.sql.Ref): Unit = ???
  def updateRef(x$1: Int,x$2: java.sql.Ref): Unit = ???
  def updateRow(): Unit = ???
  def updateRowId(x$1: String,x$2: java.sql.RowId): Unit = ???
  def updateRowId(x$1: Int,x$2: java.sql.RowId): Unit = ???
  def updateSQLXML(x$1: String,x$2: java.sql.SQLXML): Unit = ???
  def updateSQLXML(x$1: Int,x$2: java.sql.SQLXML): Unit = ???
  def updateShort(x$1: String,x$2: Short): Unit = ???
  def updateShort(x$1: Int,x$2: Short): Unit = ???
  def updateString(x$1: String,x$2: String): Unit = ???
  def updateString(x$1: Int,x$2: String): Unit = ???
  def updateTime(x$1: String,x$2: java.sql.Time): Unit = ???
  def updateTime(x$1: Int,x$2: java.sql.Time): Unit = ???
  def updateTimestamp(x$1: String,x$2: java.sql.Timestamp): Unit = ???
  def updateTimestamp(x$1: Int,x$2: java.sql.Timestamp): Unit = ???
  def wasNull(): Boolean = ???
  
  // Members declared in java.sql.Wrapper
  def isWrapperFor(x$1: Class[_]): Boolean = ???
  def unwrap[T](x$1: Class[T]): T = ???
  
    def execute(statement: String): scala.util.Try[String] = {
    res = s.executeQuery(statement) match {
     case Failure(e) => {
       logger.error(s"Querry error: ${e}")
       None
     }
     case Success(r) => Some(r)
   }
    Try[String]("42")
  }
  
  def execute(statement: java.sql.PreparedStatement): scala.util.Try[java.sql.ResultSet] = ???
  
  // Members declared in scray.jdbc.sync.JDBCDbSession
  def execute[A, S <: slick.dbio.NoStream, E <: slick.dbio.Effect](statement: slick.dbio.DBIOAction[A,S,E]): Unit = ???
  def execute[A, B <: slick.dbio.NoStream, C <: Nothing](statement: slick.sql.FixedSqlAction[A,B,C]): scala.util.Try[A] = ???
  def executeQuery(statement: String): scala.util.Try[java.sql.ResultSet] = {
    res = s.executeQuery(statement) match {
     case Failure(e) => {
       logger.error(s"Querry error: ${e}")
       None
     }
     case Success(r) => Some(r)
   }
    ""
    Try[java.sql.ResultSet](null)
  }
  override def insert(statement: java.sql.PreparedStatement): scala.util.Try[java.sql.ResultSet] = ???
  def printStatement(s: java.sql.Statement): String = ???
  override def getConnectionInformations: Option[JdbcProfile] = ???

 
}