package org.scray.sync.rest;

import java.util.List;

public record SearchRequest(
    String filter,
    List<String> sort,
    Integer limit,
    Integer offset,
    List<String> fields
) {}
