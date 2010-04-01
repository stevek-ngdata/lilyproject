package org.lilycms.linkmgmt;

import org.lilycms.hbaseindex.*;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.Repository;

import java.io.IOException;
import java.util.*;

public class LinkManager {
    private IdGenerator idGenerator;
    private static ThreadLocal<Index> FORWARD_INDEX;
    private static ThreadLocal<Index> BACKWARD_INDEX;

    public LinkManager(final IndexManager indexManager, Repository repository) throws IndexNotFoundException, IOException {
        this.idGenerator = repository.getIdGenerator();

        FORWARD_INDEX = new ThreadLocal<Index>() {
            @Override
            protected Index initialValue() {
                try {
                    return indexManager.getIndex("links-forward");
                } catch (Exception e) {
                    throw new RuntimeException("Error accessing forward links index.", e);
                }
            }
        };

        BACKWARD_INDEX = new ThreadLocal<Index>() {
            @Override
            protected Index initialValue() {
                try {
                    return indexManager.getIndex("links-backward");
                } catch (Exception e) {
                    throw new RuntimeException("Error accessing backward links index.", e);
                }
            }
        };
    }

    public void updateLinks(RecordId sourceRecord, String vtag, Set<FieldedLink> links) throws IOException {
        byte[] sourceAsBytes = sourceRecord.toBytes();

        // Read links from the forwards table
        Set<FieldedLink> oldLinks = getFieldedLinks(sourceRecord, vtag);

        // Delete existing entries from the backwards table
        for (FieldedLink link : oldLinks) {
            IndexEntry entry = createBackwardIndexEntry(vtag, link.getRecordId(), link.getFieldTypeId());
            BACKWARD_INDEX.get().removeEntry(entry, sourceAsBytes);
        }

        // Delete existing entries from the forwards table
        for (FieldedLink link : oldLinks) {
            IndexEntry entry = createForwardIndexEntry(vtag, sourceRecord, link.getFieldTypeId());
            FORWARD_INDEX.get().removeEntry(entry, link.getRecordId().toBytes());
        }

        // TODO take care of the put after delete problem: http://search-hadoop.com/m/rNnhN15Xecu

        // Store links in the forwards table
        for (FieldedLink link : links) {
            IndexEntry entry = createForwardIndexEntry(vtag, sourceRecord, link.getFieldTypeId());
            FORWARD_INDEX.get().addEntry(entry, link.getRecordId().toBytes());
        }

        // Store links in the backwards table
        for (FieldedLink link : links) {
            IndexEntry entry = createBackwardIndexEntry(vtag, link.getRecordId(), link.getFieldTypeId());
            BACKWARD_INDEX.get().addEntry(entry, sourceAsBytes);
        }
    }

    private IndexEntry createBackwardIndexEntry(String vtag, RecordId target, String sourceField) {
        IndexEntry entry = new IndexEntry();

        entry.addField("vtag", vtag);
        entry.addField("target", target.getMaster().toString());
        entry.addField("targetvariant", formatVariantProps(target.getVariantProperties()));
        entry.addField("sourcefield", sourceField);

        entry.addData("sourcefield", sourceField);

        return entry;
    }

    private IndexEntry createForwardIndexEntry(String vtag, RecordId source, String sourceField) {
        IndexEntry entry = new IndexEntry();

        entry.addField("vtag", vtag);
        entry.addField("source", source.getMaster().toString());
        entry.addField("sourcevariant", formatVariantProps(source.getVariantProperties()));
        entry.addField("sourcefield", sourceField);

        entry.addData("sourcefield", sourceField);

        return entry;
    }

    public Set<RecordId> getReferrers(RecordId record, String vtag) throws IOException {
        Query query = new Query();
        query.addEqualsCondition("vtag", vtag);
        query.addEqualsCondition("target", record.getMaster().toString());
        query.addEqualsCondition("targetvariant", formatVariantProps(record.getVariantProperties()));

        Set<RecordId> result = new HashSet<RecordId>();

        QueryResult qr = BACKWARD_INDEX.get().performQuery(query);
        byte[] id;
        while ((id = qr.next()) != null) {
            result.add(idGenerator.fromBytes(id));
        }

        return result;
    }

    public Set<FieldedLink> getFieldedReferrers(RecordId record, String vtag) throws IOException {
        Query query = new Query();
        query.addEqualsCondition("vtag", vtag);
        query.addEqualsCondition("target", record.getMaster().toString());
        query.addEqualsCondition("targetvariant", formatVariantProps(record.getVariantProperties()));

        Set<FieldedLink> result = new HashSet<FieldedLink>();

        QueryResult qr = BACKWARD_INDEX.get().performQuery(query);
        byte[] id;
        while ((id = qr.next()) != null) {
            String sourceField = qr.getDataAsString("sourcefield");
            result.add(new FieldedLink(idGenerator.fromBytes(id), sourceField));
        }

        return result;
    }

    public Set<FieldedLink> getFieldedLinks(RecordId record, String vtag) throws IOException {
        Query query = new Query();
        query.addEqualsCondition("vtag", vtag);
        query.addEqualsCondition("source", record.getMaster().toString());
        query.addEqualsCondition("sourcevariant", formatVariantProps(record.getVariantProperties()));

        Set<FieldedLink> result = new HashSet<FieldedLink>();

        QueryResult qr = FORWARD_INDEX.get().performQuery(query);
        byte[] id;
        while ((id = qr.next()) != null) {
            String sourceField = qr.getDataAsString("sourcefield");
            result.add(new FieldedLink(idGenerator.fromBytes(id), sourceField));
        }

        return result;
    }

    private String formatVariantProps(SortedMap<String, String> props) {
        if (props.isEmpty())
            return null;

        // This string-formatting logic is similar to what is in VariantRecordId, which at the time of
        // this writing was decided to keep private.
        boolean first = true;
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> prop : props.entrySet()) {
            if (!first) {
                builder.append(":");
            }
            builder.append(prop.getKey());
            builder.append(",");
            builder.append(prop.getValue());
            first = false;
        }

        return builder.toString();
    }

    public static void createIndexes(IndexManager indexManager) throws IOException {
        {
            IndexDefinition indexDef = new IndexDefinition("links-backward");
            indexDef.addStringField("vtag");
            indexDef.addStringField("target");
            indexDef.addStringField("targetvariant");
            indexDef.addStringField("sourcefield");
            indexManager.createIndex(indexDef);
        }

        {
            IndexDefinition indexDef = new IndexDefinition("links-forward");
            indexDef.addStringField("vtag");
            indexDef.addStringField("source");
            indexDef.addStringField("sourcevariant");
            indexDef.addStringField("sourcefield");
            indexManager.createIndex(indexDef);
        }
    }
}
