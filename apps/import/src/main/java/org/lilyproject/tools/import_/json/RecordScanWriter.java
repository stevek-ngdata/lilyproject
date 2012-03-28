package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.filters.RecordFilterJsonConverters;
import org.lilyproject.util.json.JsonFormat;

public class RecordScanWriter implements EntityWriter<RecordScan> {
    public static final RecordScanWriter INSTANCE = new RecordScanWriter();

    @Override
    public ObjectNode toJson(RecordScan entity, WriteOptions options, Repository repository)
            throws RepositoryException, InterruptedException {
        Namespaces namespaces = new NamespacesImpl();

        ObjectNode node = toJson(entity, options, namespaces, repository);

        node.put("namespaces", NamespacesConverter.toJson(namespaces));

        return node;
    }

    @Override
    public ObjectNode toJson(RecordScan scan, WriteOptions options, Namespaces namespaces, Repository repository)
            throws RepositoryException, InterruptedException {

        ObjectNode node = JsonFormat.OBJECT_MAPPER.createObjectNode();
        
        if (scan.getStartRecordId() != null) {
            node.put("startRecordId", scan.getStartRecordId().toString());
        }

        if (scan.getStopRecordId() != null) {
            node.put("stopRecordId", scan.getStopRecordId().toString());
        }

        if (scan.getRawStartRecordId() != null) {
            node.put("rawStartRecordId", scan.getRawStartRecordId());
        }

        if (scan.getRawStopRecordId() != null) {
            node.put("rawStopRecordId", scan.getRawStopRecordId());
        }
        
        if (scan.getRecordFilter() != null) {
            node.put("recordFilter", RecordFilterJsonConverters.INSTANCE.toJson(scan.getRecordFilter(),
                    namespaces, repository, RecordFilterJsonConverters.INSTANCE));
        }
        
        if (scan.getReturnFields() != null) {
            ObjectNode returnFieldsNode = node.putObject("returnFields");
            returnFieldsNode.put("type", scan.getReturnFields().getType().toString());
            if (scan.getReturnFields().getFields() != null) {
                ArrayNode fieldsArray = returnFieldsNode.putArray("fields");
                for (QName name : scan.getReturnFields().getFields()) {
                    fieldsArray.add(QNameConverter.toJson(name, namespaces));
                }
            }
        }
        
        node.put("caching", scan.getCaching());
        
        node.put("cacheBlocks", scan.getCacheBlocks());

        return node;
    }
}
