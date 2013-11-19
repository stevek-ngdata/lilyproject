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
     * Allows to add extra permissions on a per-request basis, independent from the permissions the user
     * has through its roles. This is useful if an application is dependent on the presence of e.g. some
     * system columns which should always be read or written, independent of the permissions of the user.
     *
     * <p>The value of the attribute should be encoded using {@link #serialize}, in contrast to some
     * other parameters it is not a CSV string.</p>
     *
     * <p>This was added to support the system columns of Lily DR.</p>
     */
    public static final String EXTRA_PERMISSION_ATT = "lily.sec.perm";

    /**
     * When true (the default), Put's will be filtered to silently remove any columns to which the user doesn't
     * have write permission. If you rather have an exception thrown instead, set this attribute to false.
     *
     * <p>The value of the attribute should be a 't' for true and a 'f' for false. Leave out the attribute
     * for the default behavior (true).</p>
     */
    public static final String FILTER_PUT_ATT = "lily.sec.filterputs";

    /**
     * HTableDescriptor key into which the column family is stored that contains the security label.
     * See also {@link #SECURITY_LABEL_QUALIFIER_KEY}.
     */
    public static final byte[] SECURITY_LABEL_FAMILY_KEY = Bytes.toBytes("lily.sec.label.family");

    /**
     * HTableDescriptor key into which the qualifier is stored that contains the security label.
     * See also {@link #SECURITY_LABEL_FAMILY_KEY}.
     */
    public static final byte[] SECURITY_LABEL_QUALIFIER_KEY = Bytes.toBytes("lily.sec.label.qualifier");

    /**
     * HTableDescriptor key into which the type of encoding which is used for serializing the security
     * label is stored. This is optional.
     *
     * See also {@link #SECURITY_LABEL_FAMILY_KEY}.
     */
    public static final byte[] SECURITY_LABEL_ENCODING_TYPE_KEY = Bytes.toBytes("lily.sec.label.encoding");

    /**
     * @see {@link #APP_NAME_ATT}
     */
    public static void setApplication(String appName, OperationWithAttributes op) {
        op.setAttribute(APP_NAME_ATT, Bytes.toBytes(appName));
    }

    /**
     * @see {@link #EXTRA_PERMISSION_ATT}
     */
    public static void setExtraPermissions(Set<String> permissions, OperationWithAttributes op) {
        op.setAttribute(EXTRA_PERMISSION_ATT, serialize(permissions));
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
