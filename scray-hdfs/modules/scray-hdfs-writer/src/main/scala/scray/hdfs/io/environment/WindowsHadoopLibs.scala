package scray.hdfs.io.environment

import com.typesafe.scalalogging.LazyLogging
import java.io.File

object WindowsHadoopLibs extends LazyLogging {
  private var dummyHadoopHomeDirWasSet = false;
  
    def createDummyWinutilsIfNotExists(path: String) = synchronized {

    if(path.trim().toLowerCase().startsWith("hdfs://")) {
      if (System.getProperty("HADOOP_HOME") == null && System.getProperty("hadoop.home.dir") == null) {
        logger.debug("No winutils.exe found. For details see https://wiki.apache.org/hadoop/WindowsProblems")
  
        val bisTmpFiles = System.getProperty("BISAS_TEMP")
        if (bisTmpFiles == null) {
          this.createWinutilsDummy(System.getProperty("java.io.tmpdir"))
        } else {
          this.createWinutilsDummy(bisTmpFiles)
        }
      }
    }
    dummyHadoopHomeDirWasSet = true;
  }
    
  private def createWinutilsDummy(basepath: String): Unit = {
    // Create dummy file if real WINUTILS.EXE is not required.
    val dummyFile = new File(basepath + "\\HADOOP_HOME");

    logger.debug(s"No WINUTILS.EXE found. But is not required for hdfs:// connections. Create dyummy file in ${dummyFile}")

    System.getProperties().put("hadoop.home.dir", dummyFile.getAbsolutePath());
    dummyFile.mkdir()
    
    new File(dummyFile.getAbsolutePath + "\\" + "bin").mkdir();
    new File(dummyFile.getAbsolutePath + "\\" + "bin" + "\\winutils.exe").createNewFile();
  }
}