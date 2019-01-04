package scray.hdfs.io.environment

import com.typesafe.scalalogging.LazyLogging
import java.io.File
import scray.hdfs.io.write.ScrayListenableFuture

object WindowsHadoopLibs extends LazyLogging {

  def createDummyWinutilsIfNotExists: ScrayListenableFuture[Boolean] = synchronized {

    try {
      if (System.getProperty("HADOOP_HOME") == null && System.getProperty("hadoop.home.dir") == null) {
        logger.debug("No winutils.exe found. For details see https://wiki.apache.org/hadoop/WindowsProblems")

        val bisTmpFiles = System.getProperty("BISAS_TEMP")
        if (bisTmpFiles == null) {
          new ScrayListenableFuture(this.createWinutilsDummy(System.getProperty("java.io.tmpdir")))
        } else {
          new ScrayListenableFuture(this.createWinutilsDummy(bisTmpFiles))
        }
      } else {
        
        if(System.getProperty("HADOOP_HOME") != null) {
          logger.debug(s"Env variable HADOOP_HOME exits ${System.getProperty("HADOOP_HOME")}. Nothing todo")

        }
        if(System.getProperty("hadoop.home.dir") != null) {
          logger.debug(s"Env variable hadoop.home.dir exits ${System.getProperty("hadoop.home.dir")}. Nothing todo")
        }
        
        new ScrayListenableFuture(true)
      }
    } catch {
      case e: Throwable => {
        logger.warn(s"Error while create dummy hadoop home" + e)
        new ScrayListenableFuture(e)
      }
    }
  }

  private def createWinutilsDummy(basepath: String): Boolean = {
    // Create dummy file if real WINUTILS.EXE is not required.
    val dummyFile = new File(basepath + "\\HADOOP_HOME");

    logger.debug(s"No WINUTILS.EXE found. But is not required for hdfs:// connections. Create dyummy file in ${dummyFile}")

    System.getProperties().put("hadoop.home.dir", dummyFile.getAbsolutePath());
    dummyFile.mkdir()

    new File(dummyFile.getAbsolutePath + "\\" + "bin").mkdir();
    new File(dummyFile.getAbsolutePath + "\\" + "bin" + "\\winutils.exe").createNewFile();
    
    true
  }
}