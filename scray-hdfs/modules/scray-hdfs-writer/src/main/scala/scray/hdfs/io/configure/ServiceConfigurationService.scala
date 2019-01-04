package scray.hdfs.io.configure

import scray.hdfs.io.write.ScrayListenableFuture

trait ServiceConfigurationService {
  def createDymmyHadoopHome: ScrayListenableFuture[Boolean]
}