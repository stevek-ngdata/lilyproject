package org.lilyproject.indexer.derefmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.hbaseindex.filter.IndexFilter;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.ByteArrayKey;

/**
 * Index filter (hbase-index) used when filtering results from a query on the dereference map.
 *
 * @author Jan Van Besien
 */
public class DerefMapIndexFilter extends IndexFilter {

    private static final byte[] FIELDS_KEY = Bytes.toBytes("fields");

    private Set<SchemaId> queriedFields;

    private Map<String, String> dependencyRecordVariantProperties;

    private final DerefMapSerializationUtil serializationUtil = new DerefMapSerializationUtil(new IdGeneratorImpl());

    public DerefMapIndexFilter() {
        // hadoop serialization
    }

    /**
     * @param dependencyRecordVariantProperties
     *                      the dependency record variant properties, used to match the variant property pattern
     *                      with
     * @param queriedFields the queried fields, used to match with the field information stored in the deref
     *                      map (the queried fields is allowed to be <code>null</code> in order to only match
     *                      results that express dependencies that do not go to a field)
     */
    DerefMapIndexFilter(Map<String, String> dependencyRecordVariantProperties,
                        Set<SchemaId> queriedFields) {
        super(Sets.newHashSet(new ByteArrayKey(FIELDS_KEY)), Sets.newHashSet("variant_properties_pattern"));

        this.queriedFields = queriedFields;
        this.dependencyRecordVariantProperties = dependencyRecordVariantProperties;
    }

    @Override
    public boolean filterData(ByteArrayKey dataQualifier, byte[] data, int offset, int length) {
        if (queriedFields == null) {
            return false;
        } else {
            if (Arrays.equals(dataQualifier.getKey(), FIELDS_KEY)) {
                final Set<SchemaId> dependencyFields = this.serializationUtil.deserializeFields(data, offset, length);

                if (!containsAtLeastOneElementOf(dependencyFields, queriedFields)) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public boolean filterField(String name, Object value) {
        if ("variant_properties_pattern".equals(name)) {
            final DerefMapVariantPropertiesPattern variantPropertiesPattern =
                    this.serializationUtil.deserializeVariantPropertiesPattern((byte[]) value);

            if (!variantPropertiesPattern.matches(dependencyRecordVariantProperties)) {
                return true;
            }
        }

        return false;
    }

    private <T> boolean containsAtLeastOneElementOf(Set<T> set, Set<T> elements) {
        set.retainAll(elements);
        return !set.isEmpty();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        if (queriedFields == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(queriedFields.size());
            for (SchemaId queriedField : queriedFields) {
                final byte[] bytes = queriedField.getBytes();
                out.writeInt(bytes.length);
                out.write(bytes);
            }
        }

        out.writeInt(dependencyRecordVariantProperties.size());
        for (Map.Entry<String, String> entry : dependencyRecordVariantProperties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        final int queriedFieldsLength = in.readInt();
        if (queriedFieldsLength != -1) {
            queriedFields = new HashSet<SchemaId>(queriedFieldsLength);
            for (int i = 0; i < queriedFieldsLength; i++) {
                final int l = in.readInt();
                final byte[] bytes = new byte[l];
                in.readFully(bytes);
                queriedFields.add(this.serializationUtil.deserializeSchemaId(bytes));
            }
        }

        final int dependencyRecordVariantPropertiesLength = in.readInt();
        dependencyRecordVariantProperties = new HashMap<String, String>();
        for (int i = 0; i < dependencyRecordVariantPropertiesLength; i++) {
            dependencyRecordVariantProperties.put(in.readUTF(), in.readUTF());
        }
    }
}
