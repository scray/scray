package org.scray.sync.rest;

import java.util.HashSet;
import java.util.Set;

import org.scray.sync.rest.exception.BadFilterException;
import org.scray.sync.rest.exception.UnsupportedFilterException;

import scray.sync.api.QuerySpec;

public class FilterParser {

	/**
	 * Very small parser: - clauses separated by ';' - each clause is field==value -
	 * value may be quoted with double quotes; quotes are removed
	 */
	public QuerySpec parse(String filter) {

		Set<String> allowedFields = new HashSet<>();
		allowedFields.add("data.processingEnv");
		allowedFields.add("data.state");
		if (filter == null || filter.isBlank()) {
			throw new BadFilterException("Filter must be a non-empty string.");
		}
		QuerySpec spec = new QuerySpec();
		String[] clauses = filter.split(";");
		for (String raw : clauses) {
			String clause = raw.trim();
			int idx = clause.indexOf("==");
			if (idx <= 0 || idx == clause.length() - 2) {
				throw new BadFilterException("Malformed clause: '" + clause + "'. Expected field==value.");
			}
			String field = clause.substring(0, idx).trim();
			if (!allowedFields.contains(field)) {
				throw new UnsupportedFilterException("Field '" + field + "' is not supported by this index.");
			}
			String value = clause.substring(idx + 2).trim();
			if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
				value = value.substring(1, value.length() - 1);
			}
			spec.add(field, "==", value);
		}
		return spec;
	}
}
