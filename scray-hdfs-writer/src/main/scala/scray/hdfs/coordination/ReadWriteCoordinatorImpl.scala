package scray.hdfs.coordination

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntUnaryOperator
import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.MutableList
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Buffer

class CompactionState {}
case object FFF extends CompactionState
case object IsReadyForCompaction extends CompactionState
case object CompactionIsStarted extends CompactionState
case object IsCompacted extends CompactionState


case class VersionPath(path: String, version: Int, compactionState: CompactionState = FFF) 

class ReadWriteCoordinatorImpl(basePath: String, numVersions: Int = 3) extends ReadCoordinator with WriteCoordinator with LazyLogging {

  private val writeDestinations = new HashMap[String, VersionPath]
  private val readSource = new HashMap[String, Buffer[VersionPath]]
  
  private val availableForCompaction = new HashMap[String, Buffer[VersionPath]]
  private val activeCompactions = new HashMap[String, Buffer[VersionPath]]


  def registerNewWriteDestination(queryspace: String) = {
    writeDestinations.put(queryspace, VersionPath(this.getPath(basePath, queryspace, 0), 0))
  }

  def getWriteDestination(queryspace: String): Option[String] = {
    writeDestinations.get(queryspace) match {
      case Some(basePath) => Some(basePath.path)
      case None => {
        logger.error(s"No write destination for ${queryspace}")
        None
      }
    }
  }

  def switchToNextVersion(queryspace: String) = {
    
    this.writeDestinations.get(queryspace) match {
      case Some(versionPath) => {
        
        // Move write version to read source
        val readSources = readSource.getOrElse(queryspace, new ListBuffer[VersionPath])
        
        readSources += (VersionPath(versionPath.path, versionPath.version, IsReadyForCompaction))
        readSource.put(queryspace, readSources)
        
        // Set new write destination
        val newVersion = (versionPath.version + 1) % numVersions
        writeDestinations.put(queryspace, VersionPath(this.getPath(basePath, queryspace, newVersion), newVersion))
      }
      case None => {
        logger.error(s"No write destination for ${queryspace}")
        None
      }
    }
  }
  
  def getReadSources(queryspace: String): Option[List[VersionPath]] = {
    readSource.get(queryspace) match {
      case Some(sources) => Some(sources.toList)
      case None => {
        logger.error(s"No read source for queryspace ${queryspace} found")
        None
      }
    }
  }
  
  def getDestinationsReadyForCompaction(queryspace: String): Option[List[String]] = {
    this.getReadSources(queryspace).map(_.filter(path => path.compactionState == IsReadyForCompaction).map(_.path))
  }
  
  def startOneCompaction(queryspace: String) = {

  }
   
  private def getPath(basePath: String, queryspace: String, version: Int): String = {
    if (basePath.endsWith("/")) {
      s"${basePath}scray-data-${queryspace}-v${version}/"
    } else {
      s"${basePath}/scray-data-${queryspace}-v${version}/"
    }
  }
}

