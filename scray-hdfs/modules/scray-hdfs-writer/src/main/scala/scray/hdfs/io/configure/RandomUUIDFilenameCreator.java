package scray.hdfs.io.configure;

import java.util.UUID;

public class RandomUUIDFilenameCreator implements FilenameCreator {

    @Override
    public String getNextFilename() {
        return UUID.randomUUID().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null)
            return false;

        if (getClass() != o.getClass())
            return false;

        return true;
    }
}
