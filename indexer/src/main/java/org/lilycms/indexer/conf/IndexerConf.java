package org.lilycms.indexer.conf;

import org.lilycms.repository.api.QName;

import java.util.*;

// TODO for safety should consider making some of the returned lists immutable

/**
 * The configuration for the indexer, describes how record types should be mapped
 * onto index documents.
 */
public class IndexerConf {
    private List<IndexCase> indexCases = new ArrayList<IndexCase>();
    private List<IndexField> indexFields = new ArrayList<IndexField>();
    private Set<QName> repoFieldDependencies = new HashSet<QName>();
    private List<IndexField> derefIndexFields = new ArrayList<IndexField>();
    private Map<QName, List<IndexField>> derefIndexFieldsByField = new HashMap<QName, List<IndexField>>();
    private Set<String> vtags = new HashSet<String>();

    protected void addIndexCase(IndexCase indexCase) {
        indexCases.add(indexCase);
        vtags.addAll(indexCase.getVersionTags());
    }

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

        QName fieldDep = indexField.getValue().getFieldDependency();
        if (fieldDep != null)
            repoFieldDependencies.add(fieldDep);

        if (indexField.getValue() instanceof DerefValue) {
            derefIndexFields.add(indexField);

            QName fieldName = ((DerefValue)indexField.getValue()).getTargetField();
            List<IndexField> fields = derefIndexFieldsByField.get(fieldName);
            if (fields == null) {
                fields = new ArrayList<IndexField>();
                derefIndexFieldsByField.put(fieldName, fields);
            }
            fields.add(indexField);
        }
    }

    /**
     * Checks if the supplied field type is used by one of the indexField's.
     */
    public boolean isIndexFieldDependency(QName fieldTypeName) {
        return repoFieldDependencies.contains(fieldTypeName);
    }

    /**
     * Returns all IndexField's which have a DerefValue.
     */
    public List<IndexField> getDerefIndexFields() {
        return derefIndexFields;
    }

    /**
     * Returns all IndexFields which have a DerefValue pointing to the given field name, or null if there are none.
     */
    public List<IndexField> getDerefIndexFields(QName fieldName) {
        List<IndexField> result = derefIndexFieldsByField.get(fieldName);
        return result == null ? Collections.<IndexField>emptyList() : result;
    }

    /**
     * Returns the set of all known vtags, thus all the vtags that are relevant to indexing.
     */
    public Set<String> getVtags() {
        return vtags;
    }
}
