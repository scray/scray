package scray.sync.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import scray.sync.api.VersionedData;

public class IdxTests {

	@Test
	public void createIdxTest() {
		var idx = new IndexDataEnvState();

		var vds = new HashMap<Integer, Map<Integer, VersionedData>>();
		var vd1 = new VersionedData();
		vd1.setDataSource("job1");
		vd1.setMergeKey("_");
		vd1.setData("{\"processingEnv\": \"http://scray.org/ai/jobs/env/see/ticket-project/provence-data/prod/ticket_update\", \"state\": \"UPDATED\", \"date\": \"1755284496.145853\"}");

		var vd2 = new VersionedData();
		vd2.setDataSource("job1");
		vd2.setMergeKey("_");
		vd2.setData("{\"processingEnv\": \"http://scray.org/ai/jobs/env/see/ticket-project/provence-data/prod/ticket_update\", \"state\": \"PUBLISHED\", \"date\": \"1755284496.145853\"}");


		idx.put(Optional.empty(), vd1, vds);

		// Check if vd was added to index
		var key1 = vds.keySet().iterator().next();
		assertEquals(1, vds.size());


		idx.put(Optional.of(vd1), vd2, vds);

		// Old vd element was replaced
		assertEquals(1, vds.size());
		var key2 = vds.keySet().iterator().next();


		// A new key was created
		assertNotEquals(key1, key2);
	}

	@Test
	public void multipleSoucekeysForOneIndex() {
		var idx = new IndexDataEnvState();

		var stateInfo = "{\"processingEnv\": \"http://scray.org/ai/jobs/env/see/ticket-project/provence-data/prod/ticket_update\", \"state\": \"UPDATED\", \"date\": \"1755284496.145853\"}";

		var vds = new HashMap<Integer, Map<Integer, VersionedData>>();
		var vd1 = new VersionedData();
		vd1.setDataSource("job1");
		vd1.setMergeKey("_");
		vd1.setData(stateInfo);

		var vd2 = new VersionedData();
		vd2.setDataSource("job2");
		vd2.setMergeKey("_");
		vd2.setData(stateInfo);


		idx.put(Optional.empty(), vd1, vds);
		idx.put(Optional.empty(), vd2, vds);

		var updatedDataKey = idx.getKey(
				"http://scray.org/ai/jobs/env/see/ticket-project/provence-data/prod/ticket_update",
				"UPDATED"
			).get();

		assertEquals(2L, vds.get(updatedDataKey).keySet().size());

	}
}
