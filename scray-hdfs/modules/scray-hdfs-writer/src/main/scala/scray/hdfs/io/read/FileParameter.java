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
package scray.hdfs.io.read;

public class FileParameter {
	Long fileSize    = null;
	String filePath  = null;
	String fileName  = null;
	boolean isFile   = false;
	long modificationTime = 0L;

	public FileParameter(Long fileSize, String filePath, String fileName, long modificationTime, boolean isFile) {
		super();
		this.isFile   = isFile;
		this.fileSize = fileSize;
		this.filePath = filePath;
		this.fileName = fileName;
		this.modificationTime = modificationTime;
	}

	public Long getFileSize() {
		return fileSize;
	}

	public String getFilePath() {
		return filePath;
	}

	public String getFileName() {
		return fileName;
	}
	
	public long getModificationTime() {
		return this.modificationTime;
	}
	
	public boolean isDirectory() {
		return !this.isFile;
	}
	
	public boolean isFile() {
		return this.isFile;
	}
}
