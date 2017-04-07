package scray.loader.tools

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.loader.tools.api.Column
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.TableIdentifier
import scala.annotation.tailrec

@RunWith(classOf[JUnitRunner])
class LuceneIndexStatementGeneratorImplSpecs extends WordSpec with LazyLogging {
  
  "LuceneIndexStatementGenerator " should {
    "create index statement for one column " in {
      val statementGenerator = new LuceneIndexStatementGeneratorImpl
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

      val statementGenerator = new LuceneIndexStatementGeneratorImpl
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

      val statementGenerator = new LuceneIndexStatementGeneratorImpl
      val configurationString = statementGenerator.getIndexString(
        TableIdentifier("cassandra", "ks", "cf1"),
        List(Column("col1", "string")),
        (1, 0, 0))

      assert( ! configurationString.isDefined)
    }
    " create alter table statement " in {
      val statementGenerator = new LuceneIndexStatementGeneratorImpl
      val alterStatement = statementGenerator.getAlterTableStatement(TableIdentifier("cassandra", "ks1", "col1"))
      
      assert(alterStatement == "ALTER TABLE \"ks1\".\"col1\" ADD lucene text;")
      
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