// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.sync.api;

import java.util.function.Function;

public class VersionedData {
    private final String dataSource;
    private final String mergeKey;
    private final long version;
    private final String data;

    public VersionedData(String dataSource, String mergeKey, long version, String data) {
        this.dataSource = dataSource;
        this.mergeKey = mergeKey;
        this.version = version;
        this.data = data;
    }

    public <T> T getDataAs(Function<String, T> f) {
        return f.apply(data);
    }

    public int getVersionKey() {
        return VersionedData.createVersionKey(dataSource, mergeKey);
    }

    public String getDataSource() {
        return dataSource;
    }

    public String getMergeKey() {
        return mergeKey;
    }

    public long getVersion() {
        return version;
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format("{\"dataSource\": \"%s\", \"mergeKey\": \"%s\", \"version\": %d, \"data\": \"%s\"}",
                dataSource, mergeKey, version, data);
    }

    public static int createVersionKey(String dataSource, String mergeKey) {
        return dataSource.hashCode() * 31 + mergeKey.hashCode() * 31;
    }
}