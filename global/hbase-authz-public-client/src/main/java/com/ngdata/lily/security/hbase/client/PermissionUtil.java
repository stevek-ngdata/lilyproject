package com.ngdata.lily.security.hbase.client;

import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;

import java.util.HashSet;
import java.util.Set;

public class PermissionUtil {
    public static final String ATTRIBUTE = "lily.sec.perm";

    /**
     * Allows to add extra permissions on a per-request basis, independent from the permissions the user
     * has through its roles. This is useful if an application is dependent on the presence of e.g. some
     * system columns which should always be read or written, independent of the permissions of the user.
     *
     * <p>This was added to support the system columns of Lily DR.</p>
     */
    public static void addPermissions(Set<String> permissions, OperationWithAttributes op) {
        op.setAttribute(ATTRIBUTE, serialize(permissions));
    }

    public static byte[] serialize(Set<String> permissions) {
        DataOutput builder = new DataOutputImpl();
        builder.writeVInt(permissions.size());
        for (String permission : permissions) {
            builder.writeVUTF(permission);
        }
        return builder.toByteArray();
    }

    public static Set<String> deserialize(byte[] permissionsAsBytes) {
        Set<String> permissions = new HashSet<String>();

        DataInput input = new DataInputImpl(permissionsAsBytes);
        int permCount = input.readVInt();
        for (int i = 0; i < permCount; i++) {
            permissions.add(input.readVUTF());
        }

        return permissions;
    }
}
