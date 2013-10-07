package com.ngdata.lily.security.hbase.client;

import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;

import java.util.HashSet;
import java.util.Set;

public class SecurityLabelUtil {
    public static final String ATTRIBUTE = "lily.sec.sl";

    public static void addSecurityLabels(Set<String> labels, Put put) {
        put.setAttribute(ATTRIBUTE, serialize(labels));
    }

    public static byte[] serialize(Set<String> labels) {
        DataOutput builder = new DataOutputImpl();
        builder.writeVInt(labels.size());
        for (String label : labels) {
            builder.writeVUTF(label);
        }
        return builder.toByteArray();
    }

    public static Set<String> deserialize(byte[] labelsAsBytes) {
        Set<String> labels = new HashSet<String>();

        DataInput input = new DataInputImpl(labelsAsBytes);
        int labelCount = input.readVInt();
        for (int i = 0; i < labelCount; i++) {
            labels.add(input.readVUTF());
        }

        return labels;
    }
}
