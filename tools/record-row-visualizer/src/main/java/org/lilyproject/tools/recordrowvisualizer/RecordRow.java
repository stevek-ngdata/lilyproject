package org.lilyproject.tools.recordrowvisualizer;

import org.lilyproject.repository.api.RecordId;

import java.util.*;

/**
 * The root data object passed to the template, containing information
 * about the HBase-storage of a Lily record.
 */
public class RecordRow {
    public RecordId recordId;

    public List<String> unknownColumns = new ArrayList<String>();
    public SystemFields systemFields = new SystemFields();
    public Fields fields = new Fields();

    // MQ row log
    public SortedMap<RowLogKey, List<String>> mqPayload = new TreeMap<RowLogKey, List<String>>();
    public SortedMap<RowLogKey, List<ExecutionData>> mqState = new TreeMap<RowLogKey, List<ExecutionData>>();

    // WAL row log
    public SortedMap<RowLogKey, List<String>> walPayload = new TreeMap<RowLogKey, List<String>>();
    public SortedMap<RowLogKey, List<ExecutionData>> walState = new TreeMap<RowLogKey, List<ExecutionData>>();

    public List<String> unknownColumnFamilies = new ArrayList<String>();

    private TreeSet<Long> allVersions;

    public RecordId getRecordId() {
        return recordId;
    }

    public List<String> getUnknownColumns() {
        return unknownColumns;
    }

    public Fields getFields() {
        return fields;
    }

    public SystemFields getSystemFields() {
        return systemFields;
    }

    public SortedMap<RowLogKey, List<String>> getMqPayload() {
        return mqPayload;
    }

    public SortedMap<RowLogKey, List<ExecutionData>> getMqState() {
        return mqState;
    }

    public SortedMap<RowLogKey, List<String>> getWalPayload() {
        return walPayload;
    }

    public SortedMap<RowLogKey, List<ExecutionData>> getWalState() {
        return walState;
    }

    public List<String> getUnknownColumnFamilies() {
        return unknownColumnFamilies;
    }

    public Set<Long> getAllVersions() {
        if (allVersions == null) {
            allVersions = new TreeSet<Long>();
            fields.collectVersions(allVersions);
            systemFields.collectVersions(allVersions);
        }
        return allVersions;
    }

    public int getAllVersionsLength() {
        return getAllVersions().size();
    }
}
