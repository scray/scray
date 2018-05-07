package scray.hdfs.osgi

import scray.hdfs.index.format.sequence.BinarySequenceFileWriter
import org.osgi.framework.ServiceRegistration
import org.osgi.framework.Bundle

class ServiceFactory extends org.osgi.framework.ServiceFactory[BinarySequenceFileWriter] {
  
  def getService(bundle: Bundle, reg: ServiceRegistration[BinarySequenceFileWriter]): BinarySequenceFileWriter = {
    new BinarySequenceFileWriter(s"hdfs://127.0.0.1:8020/tmp/scray-data-${System.currentTimeMillis()}")
  }
  
  def ungetService(bundle: Bundle, reg: ServiceRegistration[BinarySequenceFileWriter], writer: BinarySequenceFileWriter): Unit = {
    writer.close 
  }
}