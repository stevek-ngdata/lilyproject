package com.ngdata.lily.security.hbase.client;

import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;

import java.util.HashSet;
import java.util.Set;

public class HBaseAuthzUtil {
    /**
     * The name of the application, this identifies the set of permissions that will be active (different applications
     * running on top of HBase might use different sets of permissions).
     */
    public static final String APP_NAME_ATT = "lily.sec.app";

    /**
     * The list of active row permission types. For each of these permission types, the user should have a
     * permission rule that grants access.
     */
    public static final String ROW_PERMISSION_TYPES_ATT = "lily.sec.rowpermtypes";

    /**
     * Allows to add extra permissions on a per-request basis, independent from the permissions the user
     * has through its roles. This is useful if an application is dependent on the presence of e.g. some
     * system columns which should always be read or written, independent of the permissions of the user.
     *
     * <p>This was added to support the system columns of Lily DR.</p>
     */
    public static final String EXTRA_PERMISSION_ATT = "lily.sec.perm";

    /**
     * The security labels to be stored for new rows.
     */
    public static final String SECURITY_LABEL_ATT = "lily.sec.sl";

    /**
     * @see {@link #APP_NAME_ATT}
     */
    public static void setApplication(String appName, OperationWithAttributes op) {
        op.setAttribute(APP_NAME_ATT, Bytes.toBytes(appName));
    }

    /**
     * @see {@link #ROW_PERMISSION_TYPES_ATT}
     */
    public static void setRowPermissionTypes(Set<String> rowPermissionTypes, OperationWithAttributes op) {
        op.setAttribute(APP_NAME_ATT, serialize(rowPermissionTypes));
    }

    /**
     * @see {@link #EXTRA_PERMISSION_ATT}
     */
    public static void setExtraPermissions(Set<String> permissions, OperationWithAttributes op) {
        op.setAttribute(EXTRA_PERMISSION_ATT, serialize(permissions));
    }

    /**
     * @see {@link #SECURITY_LABEL_ATT}
     */
    public static void setSecurityLabels(Set<String> securityLabels, OperationWithAttributes op) {
        op.setAttribute(SECURITY_LABEL_ATT, serialize(securityLabels));
    }

    public static byte[] serialize(Set<String> strings) {
        DataOutput builder = new DataOutputImpl();
        builder.writeVInt(strings.size());
        for (String permission : strings) {
            builder.writeVUTF(permission);
        }
        return builder.toByteArray();
    }

    public static Set<String> deserialize(byte[] stringsAsBytes) {
        Set<String> permissions = new HashSet<String>();

        DataInput input = new DataInputImpl(stringsAsBytes);
        int permCount = input.readVInt();
        for (int i = 0; i < permCount; i++) {
            permissions.add(input.readVUTF());
        }

        return permissions;
    }
}
