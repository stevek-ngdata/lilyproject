package org.lilyproject.hbaseindex.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.lilyproject.util.ByteArrayKey;

/**
 * Allows additional filtering on query results based on the fields and data stored in the index.
 *
 * @author Jan Van Besien
 */
public abstract class IndexFilter implements Writable {

    /**
     * The data qualifiers on which we want the filtering to be triggered.
     */
    private Set<ByteArrayKey> filteredDataQualifiers;

    /**
     * The names of the index fields on which we want the filtering to be triggered.
     */
    private Set<String> fields;

    protected IndexFilter() {
        // hadoop serialization
    }

    protected IndexFilter(Set<ByteArrayKey> filteredDataQualifiers, Set<String> fields) {
        this.filteredDataQualifiers = filteredDataQualifiers;
        this.fields = fields;
    }

    /**
     * Filter the query result based on the data stored in the index.
     *
     * @param dataQualifier qualifier of the data
     * @param data          buffer containing the actual data
     * @param offset        offset into the buffer
     * @param length        length of the data in the buffer
     * @return true if the result needs to be skipped (filtered), false otherwise
     */
    public abstract boolean filterData(ByteArrayKey dataQualifier, byte[] data, int offset, int length);

    /**
     * Filter the query result based on the value of one of the index fields (typically one of the fields not used in
     * the query).
     *
     * @param name  name of the field
     * @param value value of the field, deserialized according to the format defined in the index definition for this
     *              field
     * @return true if the result needs to be skipped (filtered), false otherwise
     */
    public abstract boolean filterField(String name, Object value);

    public Set<ByteArrayKey> getFilteredDataQualifiers() {
        return filteredDataQualifiers;
    }

    public Set<String> getFields() {
        return fields;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(filteredDataQualifiers.size());
        for (ByteArrayKey dataQualifier : filteredDataQualifiers) {
            out.writeInt(dataQualifier.getKey().length);
            out.write(dataQualifier.getKey());
        }
        out.writeInt(fields.size());
        for (String field : fields) {
            out.writeUTF(field);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final int dataQualifiersSize = in.readInt();

        filteredDataQualifiers = new HashSet<ByteArrayKey>(dataQualifiersSize);

        for (int i = 0; i < dataQualifiersSize; i++) {
            final int len = in.readInt();
            final byte[] buf = new byte[len];
            in.readFully(buf);
            filteredDataQualifiers.add(new ByteArrayKey(buf));
        }

        final int fieldsSize = in.readInt();

        fields = new HashSet<String>(fieldsSize);

        for (int i = 0; i < fieldsSize; i++) {
            fields.add(in.readUTF());
        }
    }
}
