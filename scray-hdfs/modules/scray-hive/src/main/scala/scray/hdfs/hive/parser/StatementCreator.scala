

package scray.hdfs.hive.parser

import java.util.LinkedList
import java.io.InputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import java.util.LinkedList
import collection.JavaConverters._

class StatementCreator {

  def getPathToExistingTags(data: InputStream): LinkedList[String] = {
    val xmlInputFactory = XMLInputFactory.newInstance().createXMLEventReader(data)

    val pathToCurrentNode = new LinkedList[String]
    val xpathToExistingTags = new LinkedList[String]

    while (xmlInputFactory.hasNext()) {
      val next = xmlInputFactory.nextEvent()

      next.getEventType match {

        case XMLStreamConstants.START_ELEMENT => {
          pathToCurrentNode.add(next.asStartElement().getName.getLocalPart)
        }

        case XMLStreamConstants.END_ELEMENT => {
          pathToCurrentNode.removeLast()

        }
        case XMLStreamConstants.CHARACTERS => {
          val xPathToTag = pathToCurrentNode
            .asScala
            .foldLeft("")((path, nextFolder) => { s"${path}/${nextFolder}" })

          if (next.asCharacters().getData.trim().length() > 0) {
            xpathToExistingTags.add(xPathToTag)
          }
        }
        case _ => // Ignore other events
      }
    }

    xpathToExistingTags
  }

  def createExternetTableStatementForXMLData(tabeName: String, storageLocation: String, startTag: String, endTag: String, xPathToTags: List[String]): String = {
    val statement = new StringBuffer

    statement.append(s"CREATE TABLE IF NOT EXISTS ${tabeName} (")

    statement.append(xPathToTags.foldLeft("")((acc, next) => {
      if (acc.length() > 0) {
        s"`${next}` STRING,\n${acc}"
      } else {
        s"`${next}` STRING\n"
      }
    }))

    statement.append(s""")
      ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
      WITH SERDEPROPERTIES (""")

    statement.append(xPathToTags.foldLeft("")((acc, next) => {
      if (acc.length() > 0) {
        "\"column.xpath." + next + "\"=\"" + next + "/text()\",\n" + acc
      } else {
        "\"column.xpath." + next + "\"=\"" + next + "/text()\""
      }
    }))

    statement.append(s""")
      STORED AS
      INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
      OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
      LOCATION """" + storageLocation + """"
      TBLPROPERTIES (""")

    statement.append("\"xmlinput.start\"=\"" + startTag + "\",")
    statement.append("\"xmlinput.end\"=\"" + endTag + "\"")
    statement.append(");")

    statement.toString()
  }

}