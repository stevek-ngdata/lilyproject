package org.lilyproject.repository.impl.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.ArgumentValidator;

/**
 * Actual implementation of an HBase filter to filter out variant lily records. This returns all records which have
 * exactly the given variant properties (i.e. not the records which have these variant properties and some additional
 * onces).
 *
 * @author Jan Van Besien
 */
public class LilyRecordVariantFilter extends FilterBase {
    private Set<String> variantProperties;
    private final IdGeneratorImpl idGenerator = new IdGeneratorImpl();

    /**
     * @param variantProperties the variant properties that the records should have
     */
    public LilyRecordVariantFilter(final Set<String> variantProperties) {
        ArgumentValidator.notNull(variantProperties, "variantProperties");

        this.variantProperties = variantProperties;
    }

    public LilyRecordVariantFilter() {
        // for hbase readFields
    }

    public Set<String> getVariantProperties() {
        return variantProperties;
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        // note: return value true means it is NOT a result of the scanner, false otherwise

        if (buffer == null)
            return true;

        final RecordId recordId = idGenerator.fromBytes(new DataInputImpl(buffer, offset, length));

        final Set<String> recordVariantProperties = recordId.getVariantProperties().keySet();

        // check if the record has all expected variant properties
        final HashSet<String> allExpected = new HashSet<String>(variantProperties);
        allExpected.removeAll(recordVariantProperties);

        if (allExpected.isEmpty()) {
            // check if the record doesn't have other variant properties
            final HashSet<String> allFromRecord = new HashSet<String>(recordVariantProperties);
            allFromRecord.removeAll(variantProperties);

            return !allFromRecord.isEmpty();
        } else return true;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(variantProperties.size());
        for (String variantProperty : variantProperties) {
            out.writeUTF(variantProperty);
        }
    }

    public void readFields(DataInput in) throws IOException {
        final int size = in.readInt();
        variantProperties = new HashSet<String>(size);
        for (int i = 0; i < size; i++) {
            variantProperties.add(in.readUTF());
        }
    }
}
