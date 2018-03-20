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

package scray.hdfs.index.format.orc

import java.io.IOException
import java.nio.charset.StandardCharsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription
import org.apache.orc.Writer
import org.slf4j.LoggerFactory
import scray.hdfs.index.format.sequence.types.Blob
import java.io.InputStream

class ORCFileWriter(batchSize: Int = 10000) extends scray.hdfs.index.format.Writer {

  private var writer: Writer = null; // To write orc files to HDFS
  val schema: TypeDescription = TypeDescription.fromString("struct<id:string,time:bigint,data:binary>");
  private var batch: VectorizedRowBatch = null;
  private var numberOfInserts = 0
  
  val log = LoggerFactory.getLogger("scray.hdfs.index.format.orc.OrcWriter");

  def this(path: String) {
    this
    try {

      val hadoopConfig = new Configuration();
      hadoopConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
      hadoopConfig.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
      hadoopConfig.set("dfs.client.use.datanode.hostname", "true");

      
      writer = OrcFile.createWriter(new Path(path), OrcFile.writerOptions(hadoopConfig).setSchema(schema)
        .bloomFilterColumns("id").rowIndexStride(1000).compress(CompressionKind.NONE));

      batch = schema.createRowBatch();
    } catch {
      case e: IllegalArgumentException => {
        log.error(s"Incorrect path parameter ${path}, Message: ${e.getMessage()}");
        e.printStackTrace();
      }
      case e: IOException => {
        log.error(s"IOException while acessing orc file ${path}, Message: ${e.getMessage}");
        e.printStackTrace();
      }
    }
    
    numberOfInserts = numberOfInserts + 1
  }

  def insert(id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 0xFFFFF): Long = ???

  def insert(id: String, updateTime: Long, data: Array[Byte]): Long = {

    val idVector = batch.cols(0).asInstanceOf[BytesColumnVector]
    val timeVector = batch.cols(1).asInstanceOf[LongColumnVector]
    val elementBufferVector = batch.cols(2).asInstanceOf[BytesColumnVector];

    var row: Int = batch.size;
    batch.size += 1

    val idValue = id.toString().getBytes(StandardCharsets.UTF_8);

    idVector.setVal(row, idValue);
    timeVector.vector(row) = updateTime;
    elementBufferVector.setVal(row, data);

    if (batch.size == batch.getMaxSize()) {
      log.debug(s"Add to batch key:${id}. Batch size ${batch.size}")
      writer.addRowBatch(batch);
      batch.reset();
    }
    
    numberOfInserts = numberOfInserts + 1
    writer.getRawDataSize
  }
  
  override def insert(idBlob: Tuple2[String, Blob]) = ???

  def getBytesWritten: Long = {
    writer.getRawDataSize
  }
  
  def getNumberOfInserts: Int = {
    numberOfInserts
  }

  def close {
    try {
      if (batch.size != 0) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      writer.close();
    } catch {
      case e: IOException => e.printStackTrace();
    }
  }
}
