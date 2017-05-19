package scray.loader.configuration

import java.util.TimerTask
import scray.querying.description.QueryspaceConfiguration
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.common.errorhandling.ErrorHandler
import scray.loader.osgi.Activator
import scray.loader.configparser.QueryspaceConfigurationFileHandler
import scray.loader.configparser.ScrayConfiguration
import scray.loader.configparser.ScrayQueryspaceConfiguration
import scray.querying.Registry
import scray.jdbc.extractors.JDBCExtractors

/**
 * Update Queryspace configuration as soon as 
 */
class ScrayQueryspaceUpdater(scrayConfiguration: ScrayConfiguration, errorHandler: ErrorHandler) extends TimerTask with LazyLogging {
  
  override def run(): Unit = {
    JDBCExtractors.clearMetadataCache()
    val updateCallback: (String, Option[(ScrayQueryspaceConfiguration, Long, Option[Long])]) => Unit = { (qsFileName, qsInfo) =>
      qsInfo.map { qsInformation =>
        val (queryspaceConfig, version, prevVersionOpt) = qsInformation
        logger.debug(s"Updating queryspace ${queryspaceConfig.name} with version ${prevVersionOpt} by new version ${version}")
        val queryspaceOpt = Registry.getQuerySpace(queryspaceConfig.name, prevVersionOpt.map(_.toInt).getOrElse(0))
        logger.debug(s"Fetched old queryspace ${queryspaceOpt}")
        queryspaceOpt.map { queryspace =>
          try {
            val newQSOpt = queryspace.reInitialize(prevVersionOpt.map(_.toInt).getOrElse(0), queryspaceConfig, errorHandler)
            logger.debug(s"Initialized new queryspace ${newQSOpt}")
            newQSOpt.foreach { newQuerySpace =>
              logger.info(
                s"Re-Initializing queryspace ${newQuerySpace.name}, new version is ${version}")
              Registry.registerQuerySpace(newQuerySpace, Some(version.toInt))
            }
          } catch {
            case e: Exception => logger.warn(s"Exception while updating ${e.getLocalizedMessage()}")
          }
        }  
      }
    }
    try {
      QueryspaceConfigurationFileHandler.performQueryspaceUpdate(scrayConfiguration, Activator.queryspaces, Seq(updateCallback))
    } catch {
      case e: Exception => logger.warn(s"Exception while updating ${e.getLocalizedMessage()}")
    }
  }
  
  
  
}