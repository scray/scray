/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package scray.sync.impl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Optional;

import scray.sync.api.VersionedData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileVersionedDataApiImplTest {

    @BeforeAll
    static void ensureTargetDir() {
        new File("target").mkdirs();
    }

    @Test
    void updateVersions() {
        FileVersionedDataApiImpl fs = new FileVersionedDataApiImpl();

        fs.updateVersion("online", "key", 1L, "");
        Optional<VersionedData> v1 = fs.getLatestVersion("online", "key");
        assertTrue(v1.isPresent());
        assertEquals(1L, v1.get().getVersion());

        fs.updateVersion("online", "key", 2L, "");
        Optional<VersionedData> v2 = fs.getLatestVersion("online", "key");
        assertTrue(v2.isPresent());
        assertEquals(2L, v2.get().getVersion());
    }

    @Test
    void persistStateToLocalFile() {
        // Persist example data
        FileVersionedDataApiImpl syncInstanceCreate = new FileVersionedDataApiImpl();
        syncInstanceCreate.updateVersion(
                "http://scray.org/resourc/sync/source/online",
                "date",
                1L,
                "{\"date\": \"1234\", \"topic\": \"topic_01\", \"partition\": 0, \"offset\": 4711}"
        );
        syncInstanceCreate.updateVersion(
                "http://scray.org/resourc/sync/source/batch",
                "date",
                1L,
                "{\"date\": \"1234\", \"file\": \"hdfs://hdfs.scray.org/test/1bw2CYTuNj.seq\"}"
        );

        String path = "target/FileVersionedDataApiImplSpecs_persist.json";
        syncInstanceCreate.persist(path);

        // Read persisted data
        FileVersionedDataApiImpl syncInstanceRead = new FileVersionedDataApiImpl();
        syncInstanceRead.load(path);

        assertEquals(
                1L,
                syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/online", "date").get().getVersion()
        );
        assertEquals(
                1L,
                syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/batch", "date").get().getVersion()
        );
    }

    @Test
    void persistStateWithInputOutputStream() throws Exception {
        // Persist example data
        FileVersionedDataApiImpl syncInstanceCreate = new FileVersionedDataApiImpl();
        syncInstanceCreate.updateVersion(
                "http://scray.org/resourc/sync/source/online",
                "date",
                1L,
                "{\"date\": \"1234\", \"topic\": \"topic_01\", \"partition\": 0, \"offset\": 4711}"
        );
        syncInstanceCreate.updateVersion(
                "http://scray.org/resourc/sync/source/batch",
                "date",
                1L,
                "{\"date\": \"1234\", \"file\": \"hdfs://hdfs.scray.org/test/1bw2CYTuNj.seq\"}"
        );

        String outPath = "target/FileVersionedDataApiImplSpecs_persist_stream.json";
        try (FileOutputStream out = new FileOutputStream(outPath)) {
            syncInstanceCreate.persist(out);
        }

        // Read persisted data
        FileVersionedDataApiImpl syncInstanceRead = new FileVersionedDataApiImpl();
        try (FileInputStream in = new FileInputStream(new File(outPath))) {
            syncInstanceRead.load(in);
        }

        assertEquals(
                1L,
                syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/online", "date").get().getVersion()
        );
        assertEquals(
                1L,
                syncInstanceRead.getLatestVersion("http://scray.org/resourc/sync/source/batch", "date").get().getVersion()
        );
    }
}
