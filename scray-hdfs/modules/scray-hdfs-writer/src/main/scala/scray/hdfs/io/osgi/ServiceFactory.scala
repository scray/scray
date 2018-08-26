// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.hdfs.io.osgi

import scray.hdfs.io.index.format.sequence.BinarySequenceFileWriter
import org.osgi.framework.ServiceRegistration
import org.osgi.framework.Bundle
import scray.hdfs.io.coordination.IHdfsWriterConstats
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable

class ServiceFactory extends org.osgi.framework.ServiceFactory[BinarySequenceFileWriter[Text, Text, Text, BytesWritable]] {
  
  def getService(bundle: Bundle, reg: ServiceRegistration[BinarySequenceFileWriter[Text, Text, Text, BytesWritable]]): BinarySequenceFileWriter[Text, Text, Text, BytesWritable] = {
    val destinationPath = bundle.getBundleContext.getProperty(IHdfsWriterConstats.WriteParameter.destinationPath.toString)
    if(destinationPath.endsWith(java.io.File.separator)) {
      new BinarySequenceFileWriter(destinationPath  + System.currentTimeMillis() + ".seq", new OutputTextBytesWritable)
    } else {
      new BinarySequenceFileWriter(destinationPath + java.io.File.separator + System.currentTimeMillis() + ".seq", new OutputTextBytesWritable)
    }
  }
  
  def ungetService(bundle: Bundle, reg: ServiceRegistration[BinarySequenceFileWriter[Text, Text, Text, BytesWritable]], writer: BinarySequenceFileWriter[Text, Text, Text, BytesWritable]): Unit = {
    writer.close 
  }
}