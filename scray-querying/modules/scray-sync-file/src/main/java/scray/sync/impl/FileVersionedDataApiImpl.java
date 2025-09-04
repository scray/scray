package scray.sync.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import scala.Array;
import scray.sync.api.VersionedData;
import scray.sync.api.VersionedDataApi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class FileVersionedDataApiImpl implements VersionedDataApi {
    private static final Logger logger = LoggerFactory.getLogger(FileVersionedDataApiImpl.class);

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private Map<Integer, VersionedData> versionInformations = new ConcurrentHashMap<>();
    private Map<Integer, Map<Integer, VersionedData>> versionInformationsIdxEnvState = new ConcurrentHashMap<>();
    private Idx idx = new IndexDataEnvState();

    private IndexDataEnvState indexCreator = new IndexDataEnvState();


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

        // Get old version to know which one to update
    	var oldV = Optional.ofNullable(versionInformations.get(vd.getVersionKey()));
    	// Add new state to idx
        idx.put(oldV, vd, versionInformationsIdxEnvState);

        versionInformations.put(vd.getVersionKey(), vd);
    }

    public void updateVersion(VersionedData vd) {
        // Get old version to know which one to update
    	var oldV = Optional.ofNullable(versionInformations.get(vd.getVersionKey()));
    	// Add new state to idx
        idx.put(oldV, vd, versionInformationsIdxEnvState);

        versionInformations.put(vd.getVersionKey(), vd);
    }

	@Override
	public Optional<List<VersionedData>> getLatestVersion(String idxName, String attribute1, String attribute2) {

		 Optional<Integer> key = idx.getKey(attribute1, attribute2);

		 if(key.isEmpty()) {
			 logger.debug("Error while creating key from inputdata");
			 return Optional.empty();
		 } else {
			var vds = Optional.ofNullable(versionInformationsIdxEnvState.get(key.get()))
			.map(Map::values);


			if(vds.isEmpty()) {
				return Optional.empty();
			} else {
				return Optional.of(new ArrayList(vds.get()));
			}

		 }
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
        Map<Integer, VersionedData> resultMap = new ConcurrentHashMap<>();
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
