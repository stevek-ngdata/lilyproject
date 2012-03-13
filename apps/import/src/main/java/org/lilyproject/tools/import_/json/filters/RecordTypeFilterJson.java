package org.lilyproject.tools.import_.json.filters;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.tools.import_.json.QNameConverter;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class RecordTypeFilterJson implements RecordFilterJsonConverter<RecordTypeFilter> {
    @Override
    public boolean supports(String typeName) {
        return typeName.equals(RecordTypeFilter.class.getName());
    }

    @Override
    public ObjectNode toJson(RecordTypeFilter filter, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException {

        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();
        
        if (filter.getRecordType() != null) {
            node.put("recordType", QNameConverter.toJson(filter.getRecordType(), namespaces));
        }
        
        if (filter.getVersion() != null) {
            node.put("version", filter.getVersion());
        }
        
        return node;
    }

    @Override
    public RecordTypeFilter fromJson(JsonNode node, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException {
        
        RecordTypeFilter filter = new RecordTypeFilter();

        String recordType = JsonUtil.getString(node, "recordType", null);
        if (recordType != null) {
            filter.setRecordType(QNameConverter.fromJson(recordType, namespaces));
        }
        
        Long version = JsonUtil.getLong(node, "version", null);
        if (version != null) {
            filter.setVersion(version);
        }
        
        return filter;
    }
}
