package scray.hdfs.io.osgi

import scray.hdfs.io.configure.ServiceConfigurationService
import scray.hdfs.io.environment.WindowsHadoopLibs
import scray.hdfs.io.write.ScrayListenableFuture

class ServiceConfigurationServiceImpl extends ServiceConfigurationService {
  
  def createDymmyHadoopHome: ScrayListenableFuture[Boolean] = {
    WindowsHadoopLibs.createDummyWinutilsIfNotExists
  }
}