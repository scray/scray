package scray.sync.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.api.VersionedData;
import scray.sync.api.VersionedDataApi;

public class IndexDataEnvState implements Idx {

    private static final Logger logger = LoggerFactory.getLogger(IndexDataEnvState.class);


    private Map<Integer, VersionedData> versionInformationsIdxEnvState = new ConcurrentHashMap<>();


	public Optional<Integer> getKey(VersionedData vd) {


        ObjectMapper mapper = new ObjectMapper();

        try {
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(vd.getData());

            String state = root.path("state").asText();
            String processingEnv = root.path("processingEnv").asText();

            return Optional.of(vd.createVersionKey(processingEnv, state));

        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            System.err.println("❌ Failed to parse JSON: " + e.getMessage());
		}

        logger.debug("No data to calculate an index");
		return Optional.empty();
	}

	public Optional<Integer> getKey(String processingEnv, String state) {
		return Optional.of(VersionedData.createVersionKey(processingEnv, state));
	}

	@Override
	public Map<Integer, Map<Integer, VersionedData>> put(
			Optional<VersionedData> oldV,
			VersionedData newV,
			Map<Integer, Map<Integer, VersionedData>> versionInformationsIdxEnvState
		) {

		// Remove old version if present
		oldV.flatMap(this::getKey)
	    .ifPresent(key -> {
	        var vds = versionInformationsIdxEnvState.get(key);
	        if (vds != null) {
	            vds.remove(oldV.get().getVersionKey());
	            if (vds.isEmpty()) {
	                versionInformationsIdxEnvState.remove(key);
	            }
	        }
	    });


		// Add new element if it possible to generate key
		this.getKey(newV).ifPresent(key -> {
			var idxKeyData = Optional.ofNullable(versionInformationsIdxEnvState.get(key)).orElseGet(ConcurrentHashMap::new);
			idxKeyData.put(newV.getVersionKey(), newV);
			versionInformationsIdxEnvState.put(key, idxKeyData);
		}
		);

		return versionInformationsIdxEnvState;
	}
}
