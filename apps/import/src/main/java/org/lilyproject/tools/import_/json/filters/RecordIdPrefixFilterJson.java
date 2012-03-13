package org.lilyproject.tools.import_.json.filters;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordIdPrefixFilter;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class RecordIdPrefixFilterJson implements RecordFilterJsonConverter<RecordIdPrefixFilter> {
    @Override
    public boolean supports(String typeName) {
        return typeName.equals(RecordIdPrefixFilter.class.getName());
    }

    @Override
    public ObjectNode toJson(RecordIdPrefixFilter filter, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {
        
        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();
        
        if (filter.getRecordId() != null) {
            node.put("recordId", filter.getRecordId().toString());
        }

        return node;
    }

    @Override
    public RecordIdPrefixFilter fromJson(JsonNode node, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {
        
        RecordIdPrefixFilter filter = new RecordIdPrefixFilter();

        String recordId = JsonUtil.getString(node, "recordId", null);
        if (recordId != null) {            
            filter.setRecordId(repository.getIdGenerator().fromString(recordId));
        }
        
        return filter;
    }
}
