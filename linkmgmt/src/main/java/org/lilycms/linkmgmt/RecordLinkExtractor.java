package org.lilycms.linkmgmt;

import org.lilycms.repository.api.*;
import org.lilycms.repository.api.Record.Scope;

import java.util.Map;

public class RecordLinkExtractor {
    /**
     * Extracts the links from a record. The provided Record object should
     * be "fully loaded" (= contain all fields).
     */
    public void extract(Record record, LinkCollector collector, TypeManager typeManager) throws RepositoryException {
        // TODO this will need to:
        //  run over all fields, check using the typemanager if it is a link field, if so put
        //  the resolved link into the collector.
        for(Map.Entry<String, Object> field : record.getFields(Scope.VERSIONABLE).entrySet()) {
        }

        // TODO besides extracting links from link fields, this could go further and extract links
        // from blob fields too, using an extensible set of LinkExtractors, similar to Daisy.
    }
}
