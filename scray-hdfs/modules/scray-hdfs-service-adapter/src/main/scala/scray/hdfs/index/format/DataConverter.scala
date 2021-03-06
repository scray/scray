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
package scray.hdfs.index.format

class DataConverter {
  
  /**
   * Create index and data record from the given key and value. IndexFile and BlobFile will be updated.
   */
  def addRecord(key: Array[Byte], value: Array[Byte], idxFile: IndexFile, dataFile: BlobFile) {
    val idxRecord = IndexFileRecord(key, idxFile.getLastPosition, value.length)
    val datRecord = BlobFileRecord(key,value)
    
    idxFile.addRecord(idxRecord, value.length)
    dataFile.addRecord(datRecord)
  }
  
}