package org.lilycms.indexer.conf;

import org.lilycms.repository.api.QName;

import java.util.*;

/**
 * The configuration for the indexer, describes how record types should be mapped
 * onto index documents.
 */
public class IndexerConf {
    protected List<IndexCase> indexCases = new ArrayList<IndexCase>();
    private List<IndexField> indexFields = new ArrayList<IndexField>();
    private Set<QName> repoFieldDependencies = new HashSet<QName>();

    /**
     * @return null if there is no matching IndexCase
     */
    public IndexCase getIndexCase(String recordTypeName, Map<String, String> varProps) {
        for (IndexCase indexCase : indexCases) {
            if (indexCase.match(recordTypeName, varProps)) {
                return indexCase;
            }
        }

        return null;
    }

    public List<IndexField> getIndexFields() {
        return indexFields;
    }

    protected void addIndexField(IndexField indexField) {
        indexFields.add(indexField);
        repoFieldDependencies.add(indexField.getValue().getFieldDependency());
    }

    /**
     * Checks if the supplied field type is used by one of the indexField's.
     */
    public boolean isIndexFieldDependency(QName fieldTypeName) {
        return repoFieldDependencies.contains(fieldTypeName);
    }

}
