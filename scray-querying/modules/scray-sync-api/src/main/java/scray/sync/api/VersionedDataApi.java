// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.sync.api;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

public interface VersionedDataApi {

    /**
     * Get the latest version of a resource based on dataSource and mergeKey
     */
    Optional<VersionedData> getLatestVersion(String dataSource, String mergeKey);

    /**
     * Update the versioned data for a given resource
     */
    void updateVersion(String dataSource, String mergeKey, long version, String data);

    /**
     * Persist versioned data information to the local file system
     */
    void persist(String path);

    /**
     * Write versioned data information to an OutputStream
     */
    void persist(OutputStream stream);

    /**
     * Load versioned data information from the local file system
     */
    void load(String path);

    /**
     * Load versioned data information from a given InputStream
     */
    void load(InputStream stream);

    /**
     * Get all resources where a version exists
     */
    List<VersionedData> getAllVersionedResources();
}