package org.lilyproject.tools.import_.json;

import org.apache.hadoop.hbase.filter.Filter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;
import org.lilyproject.tools.import_.json.filters.RecordFilterJsonConverters;
import org.lilyproject.util.json.JsonUtil;

import java.io.IOException;
import java.util.ServiceLoader;

public class RecordFilterReader implements EntityReader<RecordFilter> {
    public static final RecordScanReader INSTANCE = new RecordScanReader();

    @Override
    public RecordFilter fromJson(JsonNode node, Repository repository)
            throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(node, null, repository);
    }

    @Override
    public RecordFilter fromJson(JsonNode nodeNode, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!nodeNode.isObject()) {
            throw new JsonFormatException("Expected a json object for record filter, got: " +
                    nodeNode.getClass().getName());
        }

        ObjectNode node = (ObjectNode)nodeNode;

        namespaces = NamespacesConverter.fromContextJson(node, namespaces);

        return RecordFilterJsonConverters.INSTANCE.fromJson(node, namespaces, repository,
                RecordFilterJsonConverters.INSTANCE);
    }

}
