package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.filters.RecordFilterJsonConverters;

public class RecordFilterWriter implements EntityWriter<RecordFilter> {

    @Override
    public ObjectNode toJson(RecordFilter entity, WriteOptions options, Repository repository)
            throws RepositoryException, InterruptedException {
        Namespaces namespaces = new NamespacesImpl();

        ObjectNode node = toJson(entity, options, namespaces, repository);

        node.put("namespaces", NamespacesConverter.toJson(namespaces));

        return node;
    }

    @Override
    public ObjectNode toJson(RecordFilter filter, WriteOptions options, Namespaces namespaces, Repository repository)
            throws RepositoryException, InterruptedException {

        return RecordFilterJsonConverters.INSTANCE.toJson(filter, namespaces, repository,
                RecordFilterJsonConverters.INSTANCE);
    }

}
