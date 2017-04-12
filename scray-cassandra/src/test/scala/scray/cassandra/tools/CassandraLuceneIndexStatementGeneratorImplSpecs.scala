package scray.cassandra.tools

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.cassandra.tools.api.Column
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.TableIdentifier
import scala.annotation.tailrec
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.DataType.Name._
import com.datastax.driver.core.DataType.Name
import scray.cassandra.tools.CassandraLuceneIndexStatementGeneratorImpl

@RunWith(classOf[JUnitRunner])
class CassandraLuceneIndexStatementGeneratorImplSpecs extends WordSpec with LazyLogging {
  
  "LuceneIndexStatementGenerator " should {
    "create index statement for one column " in {
      val statementGenerator = new CassandraLuceneIndexStatementGeneratorImpl
      val configurationString = statementGenerator.getIndexString(
        TableIdentifier("cassandra", "ks1", "cf1"),
        List(Column("col1", "string")),
        (2, 2, 3))

      
      val expectedResult = s"""
        CREATE CUSTOM INDEX "cf1_lucene_index" ON "ks1"."cf1" (lucene)
        USING 'com.stratio.cassandra.lucene.Index'
        WITH OPTIONS = {
        	'refresh_seconds' : '1',
          'schema' : '{
            fields : { 
              col1 : {type: "string"}
		        }
	        }'
        };"""
        

      assert(configurationString.isDefined)
      assert(removePrettyPrintingChars(configurationString.getOrElse("")) == removePrettyPrintingChars(expectedResult))

    }
    "create index statement for multiple columns " in {

      val statementGenerator = new CassandraLuceneIndexStatementGeneratorImpl
      val configurationString = statementGenerator.getIndexString(
        TableIdentifier("cassandra", "ks", "cf1"),
        List(Column("col1", "string"), Column("col2", "string")),
        (2, 2, 3))

        val expectedResult = s"""
          CREATE CUSTOM INDEX "cf1_lucene_index" ON "ks"."cf1" (lucene) 
          USING 'com.stratio.cassandra.lucene.Index' 
          WITH OPTIONS = { 
	          'refresh_seconds' : '1', 
	          'schema' : '{ 
		          fields : { 
                  col1 : {type: "string"} 
                  col2 : {type: "string"}
		          }
	        }' 
        };"""
        
      assert(configurationString.isDefined)
      assert(removePrettyPrintingChars(configurationString.getOrElse("")) == removePrettyPrintingChars(expectedResult))

    }
    "create no index statement if lucene plugin version is wrong " in {

      val statementGenerator = new CassandraLuceneIndexStatementGeneratorImpl
      val configurationString = statementGenerator.getIndexString(
        TableIdentifier("cassandra", "ks", "cf1"),
        List(Column("col1", "string")),
        (1, 0, 0))

      assert( ! configurationString.isDefined)
    }
    " create alter table statement " in {
      val statementGenerator = new CassandraLuceneIndexStatementGeneratorImpl
      val alterStatement = statementGenerator.getAlterTableStatement(TableIdentifier("cassandra", "ks1", "col1"))
      
      assert(alterStatement == "ALTER TABLE \"ks1\".\"col1\" ADD lucene text;")
      
    }
    " find db type for scala classes " in {
      val string: String = ""
      val long: Long = 42L
      val int: Integer = 42
      val boolean: Boolean = false

      
      val statementGenerator = new CassandraLuceneIndexStatementGeneratorImpl
      
      assert(statementGenerator.getLuceneType(string) == "string")
      assert(statementGenerator.getLuceneType(long) == "long")
      assert(statementGenerator.getLuceneType(int) == "integer")
      assert(statementGenerator.getLuceneType(boolean) == "boolean")
    }
    " map cassandra type to Scala type" in {
      
      assert(this.CasToScalaMap(Name.ASCII) match {
        case _: String => true
        case _ => false
      })
     
      assert(this.CasToScalaMap(Name.BOOLEAN) match {
        case _: Boolean => true
        case _ => false
      })
    }
  }
  
  case class ScalaType[T]()
  
  def getDbType(hostname: String, ti: TableIdentifier, columName: String): Option[ScalaType[_]] = {
    
    if(ti.dbSystem.toLowerCase == "cassandra") {
      val session = Cluster.builder().addContactPoint("2a02:8071:3189:9700:baae:edff:fe7b:9289").build().connect();
      val datatype = session.getCluster.getMetadata.getKeyspace("tumblr").getTable("post_0").getColumn("posturl").getType.getName
      
    }
      None
    
  }
  
  def CasToScalaMap(cassandratype: Name): Any = {
    cassandratype match {
        case Name.ASCII => ""
        case Name.BIGINT => 1L
//        case Name.BLOB => ""
        case Name.BOOLEAN => true
//        case Name.COUNTER => ""
//        case Name.DECIMAL => ""
        case Name.DOUBLE => 1.0D 
        case Name.FLOAT => 1.0f
//        case Name.INET => ""
//        case Name.INT => ""
        case Name.TEXT => ""
//        case Name.TIMESTAMP => ""
//        case Name.UUID => ""
//        case Name.VARCHAR => ""
//        case Name.VARINT => ""
//        case Name.TIMEUUID => ""
//        case Name.LIST => ""
//        case Name.SET => ""
//        case Name.MAP => ""
//        case Name.CUSTOM => ""
//        case Name.UDT => ""
//        case Name.TUPLE => ""
//        case Name.SMALLINT => ""
//        case Name.TINYINT => ""
//        case Name.DATE => ""
//        case Name.TIME => ""
    }
  }
  
  private def removePrettyPrintingChars(prettyString: String): String = {
    
    @tailrec
    def removeSpaces(string: String): String = {
      if(string.contains("  ")) {
        removeSpaces(string.replace("  ", " "))
      } else {
        string
      }
    }
    
    removeSpaces(removeSpaces(prettyString).replace("\n ", "\n").replace(" \n", "\n").replace("\n", " ").replace("\t","").trim())
  }
}