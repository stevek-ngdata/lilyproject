package org.lilyproject.tools.import_.json.filters;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;

public interface RecordFilterJsonConverter<T extends RecordFilter> {
    
    boolean supports(String typeName);

    ObjectNode toJson(T filter, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws RepositoryException, InterruptedException;

    T fromJson(JsonNode node, Namespaces namespaces, Repository repository,
            RecordFilterJsonConverter<RecordFilter> converter)
            throws JsonFormatException, RepositoryException, InterruptedException;
}
