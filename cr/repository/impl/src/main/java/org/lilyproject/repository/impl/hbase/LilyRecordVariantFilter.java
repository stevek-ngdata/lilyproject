package org.lilyproject.repository.impl.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.ArgumentValidator;

/**
 * Actual implementation of an HBase filter to filter out variant lily records. This returns all records which have
 * exactly the given variant properties (i.e. not the records which have these variant properties and some additional
 * ones). If the value of the variant property is specified, it has to match exactly. If the value is
 * <code>null</code>, any value will match.
 *
 *
 */
public class LilyRecordVariantFilter extends FilterBase {
    private Map<String, String> variantProperties;
    private final IdGeneratorImpl idGenerator = new IdGeneratorImpl();

    /**
     * @param variantProperties the variant properties that the records should have
     */
    public LilyRecordVariantFilter(final Map<String, String> variantProperties) {
        ArgumentValidator.notNull(variantProperties, "variantProperties");

        this.variantProperties = variantProperties;
    }

    public LilyRecordVariantFilter() {
        // for hbase readFields
    }

    public Map<String, String> getVariantProperties() {
        return variantProperties;
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        // note: return value true means it is NOT a result of the scanner, false otherwise

        if (buffer == null)
            return true;

        final RecordId recordId = idGenerator.fromBytes(new DataInputImpl(buffer, offset, length));

        final SortedMap<String, String> recordVariantProperties = recordId.getVariantProperties();

        // check if the record has all expected variant properties
        if (containsAllExpectedDimensions(recordVariantProperties) &&
                hasSameValueForValuedDimensions(recordVariantProperties)) {

            // check if the record doesn't have other variant properties
            return variantProperties.size() != recordVariantProperties.size();
        } else return true;
    }

    private boolean containsAllExpectedDimensions(Map<String, String> recordVariantProperties) {
        return recordVariantProperties.keySet().containsAll(this.variantProperties.keySet());
    }

    private boolean hasSameValueForValuedDimensions(Map<String, String> variantProperties) {
        return variantProperties.entrySet().containsAll(getValuedDimensions().entrySet());
    }

    private Map<String, String> getValuedDimensions() {
        return Maps.filterValues(variantProperties, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input != null;
            }
        });
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(variantProperties.size());
        for (Map.Entry<String, String> variantProperty : variantProperties.entrySet()) {
            out.writeUTF(variantProperty.getKey());
            out.writeUTF(variantProperty.getValue() != null ? variantProperty.getValue() : "\u0000");
        }
    }

    public void readFields(DataInput in) throws IOException {
        final int size = in.readInt();
        variantProperties = new HashMap<String, String>(size);
        for (int i = 0; i < size; i++) {
            final String key = in.readUTF();
            final String value = in.readUTF();
            variantProperties.put(key, value.equals("\u0000") ? null : value);
        }
    }
}

