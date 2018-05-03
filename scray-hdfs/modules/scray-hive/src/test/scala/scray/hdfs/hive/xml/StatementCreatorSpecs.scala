package scray.hdfs.hive.xml

import org.scalatest.WordSpec
import com.typesafe.scalalogging.LazyLogging
import java.io.ByteArrayInputStream
import javax.xml.bind.JAXBContext
import javax.xml.stream.XMLInputFactory
import java.nio.charset.StandardCharsets
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.events.XMLEvent
import java.util.LinkedList
import collection.JavaConverters._
import scray.hdfs.hive.parser.StatementCreator
import org.scalatest.Assertions
import java.io.FileInputStream
import java.io.File

class StatementCreatorSpecs  extends WordSpec with LazyLogging {
   "StatementCreator " should {
//    " get XPath expression to all tags " in {
//      
//      val xmlData = """
//        <DELFOR>
//          <CNT>
//          <IC>
//            <TRANSMISSION_DATE>20100520</TRANSMISSION_DATE>
//            <TRANSMISSION_TIME>051600</TRANSMISSION_TIME>
//            <TEST1>
//              <TEST2>
//                "ABC"
//              </TEST2>
//            </TEST1>
//         </IC>
//         </CNT>
//        </DELFOR>         
//        """
//        val creator = new StatementCreator
//        val xPathsToTags = creator.getPathToExistingTags(new ByteArrayInputStream(xmlData.getBytes(StandardCharsets.UTF_8)))
//        
//        Assertions.assert(xPathsToTags.contains("/DELFOR/CNT/IC/TRANSMISSION_DATE"))
//        Assertions.assert(xPathsToTags.contains("/DELFOR/CNT/IC/TRANSMISSION_TIME"))
//        Assertions.assert(xPathsToTags.contains("/DELFOR/CNT/IC/TEST1/TEST2"))
//    }   
//    "create hive table create statement (xml) " in {
//      val xPaths = "/DELFOR/CNT/IC/TRANSMISSION_DATE" :: "/DELFOR/CNT/IC/TRANSMISSION_TIME" :: "/DELFOR/CNT/IC/TEST1/TEST2" :: Nil
//      
//      val creator = new StatementCreator
//      val createdTableStatement = creator.createExternetTableStatementForXMLData("Table1", "/files/", "<DELFOR", "</DELFOR>", xPaths))
//      
//      val expectedResults: String = """
//        CREATE TABLE IF NOT EXISTS Table1 (
//          `/DELFOR/CNT/IC/TEST1/TEST2` STRING,
//          `/DELFOR/CNT/IC/TRANSMISSION_TIME` STRING,
//          `/DELFOR/CNT/IC/TRANSMISSION_DATE` STRING
//          )
//        ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
//        WITH SERDEPROPERTIES (
//          "column.xpath./DELFOR/CNT/IC/TEST1/TEST2"="/DELFOR/CNT/IC/TEST1/TEST2/text()",
//          "column.xpath./DELFOR/CNT/IC/TRANSMISSION_TIME"="/DELFOR/CNT/IC/TRANSMISSION_TIME/text()",
//          "column.xpath./DELFOR/CNT/IC/TRANSMISSION_DATE"="/DELFOR/CNT/IC/TRANSMISSION_DATE/text()"
//        )
//        STORED AS
//        INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
//        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
//        LOCATION "/files/"
//        TBLPROPERTIES ("xmlinput.start"="<DELFOR","xmlinput.end"="</DELFOR>");
//        """
//      Assertions.assertEquals(expectedResults.replache(" ", ""), createdTableStatement.replache(" ", ""))
//    }
    "read xml from file" in {
      val in = new FileInputStream(new File("/home/stefan/tmp/delfor/DELPHI_GLOBAL.DELPHI_GLOBAL_DELFOR_D97A_IN.4.xml.xml"));
      
      val creator = new StatementCreator
      val xPaths = creator.getPathToExistingTags(in)
      val createdTableStatement = creator.createExternetTableStatementForXMLData("DELPHI_GLOBAL_DELPHI_GLOBAL_DELFOR_D97A_IN", "/BIS_TESTFILES/CATERPILLAR_GLOBAL.CATERPILLAR_GLOBAL_DELFOR_D98B", "<SEEDESADV", "</SEEDESADV>", xPaths.asScala.toList)
    
      println(createdTableStatement)
    }
   }
}