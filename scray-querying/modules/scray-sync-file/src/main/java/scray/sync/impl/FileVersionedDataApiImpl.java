package scray.sync.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import scray.sync.api.VersionedData;
import scray.sync.api.VersionedDataApi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FileVersionedDataApiImpl implements VersionedDataApi {
    private static final Logger logger = LoggerFactory.getLogger(FileVersionedDataApiImpl.class);

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private Map<Integer, VersionedData> versionInformations = new HashMap<>();

    public FileVersionedDataApiImpl() {
        this.versionInformations = toMap(new ArrayList<>());
    }

    @Override
    public Optional<VersionedData> getLatestVersion(String dataSource, String mergeKey) {
        int key = VersionedData.createVersionKey(dataSource, mergeKey);
        return Optional.ofNullable(versionInformations.get(key));
    }

    @Override
    public void updateVersion(String dataSource, String mergeKey, long version, String data) {
        VersionedData vd = new VersionedData(dataSource, mergeKey, version, data);
        versionInformations.put(vd.getVersionKey(), vd);
    }

    public void updateVersion(VersionedData vd) {
        versionInformations.put(vd.getVersionKey(), vd);
    }

    @Override
    public void persist(String path) {
        writeToFile(path);
    }

    @Override
    public void persist(OutputStream stream) {
        writeToStream(stream);
    }

    @Override
    public void load(String path) {
        versionInformations = readFromFile(path);
    }

    @Override
    public void load(InputStream stream) {
        versionInformations = readFromStream(stream);
    }

    @Override
    public List<VersionedData> getAllVersionedResources() {
        return new ArrayList<>(versionInformations.values());
    }

    // === Private helper methods ===

    private Map<Integer, VersionedData> readFromFile(String path) {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            Type listType = new TypeToken<List<VersionedData>>() {}.getType();
            List<VersionedData> dataList = gson.fromJson(reader, listType);
            return toMap(dataList != null ? dataList : new ArrayList<>());
        } catch (IOException e) {
            logger.debug("Unable to open file {}. Using empty version collection", path, e);
            return new HashMap<>();
        }
    }

    private Map<Integer, VersionedData> readFromStream(InputStream stream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Type listType = new TypeToken<List<VersionedData>>() {}.getType();
            List<VersionedData> dataList = gson.fromJson(reader, listType);
            return toMap(dataList != null ? dataList : new ArrayList<>());
        } catch (IOException e) {
            logger.debug("Unable to read from stream. Using empty version collection", e);
            return new HashMap<>();
        }
    }

    private Map<Integer, VersionedData> toMap(List<VersionedData> dataList) {
        Map<Integer, VersionedData> resultMap = new HashMap<>();
        for (VersionedData data : dataList) {
            int key = data.getVersionKey();
            VersionedData existing = resultMap.get(key);
            if (existing == null || data.getVersion() >= existing.getVersion()) {
                resultMap.put(key, data);
            } else {
                logger.debug("Existing version is newer: existing={}, incoming={}", existing, data);
            }
        }
        return resultMap;
    }

    private void writeToFile(String path) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            gson.toJson(getAllVersionedResources(), writer);
        } catch (IOException e) {
            logger.error("Failed to write to file: {}", path, e);
        }
    }

    private void writeToStream(OutputStream stream) {
        try (Writer writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8)) {
            gson.toJson(getAllVersionedResources(), writer);
            writer.flush();
        } catch (IOException e) {
            logger.error("Failed to write to output stream", e);
        }
    }
}
