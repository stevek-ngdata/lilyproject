package com.ngdata.lily.security.hbase.client;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * Information about a user as used within the HBase authorization framework.
 *
 * <p>This can be seen as the subset of information held by the AuthenticationContext which is needed
 * to perform authorization on the HBase level.</p>
 */
public class AuthorizationContext {
    private String name;
    private String tenant;
    private Set<String> roles;

    /**
     * The attribute on a HBase operation (like Get, Put, Scan, etc) in which the authentication
     * information (= the serialization of this object) is stored.
     */
    public static final String OPERATION_ATTRIBUTE = "lily.authctx";

    /**
     * Constructor.
     *
     * @param name name of this user, optional (nullable), only for informational purposes
     * @param tenant unique name/id of the tenant for which the user is current logged in
     * @param roles roles of the user for the active tenant, <b>without the tenant component</b>
     */
    public AuthorizationContext(@Nullable String name, @Nonnull String tenant, @Nonnull Set<String> roles) {
        this.name = name;
        this.tenant = tenant;
        this.roles = roles;
    }

    /**
     * The name of the user, this is only used for informational/debugging purposes.
     *
     * @return null if the user is unknown
     */
    @Nullable
    public String getName() {
        return name;
    }

    public String getTenant() {
        return tenant;
    }

    /**
     * The roles of the user.
     */
    public Set<String> getRoles() {
        return roles;
    }

    public byte[] serialize() {
        DataOutput buffer = new DataOutputImpl();

        buffer.writeVUTF(name);
        buffer.writeVUTF(tenant);

        buffer.writeVInt(roles.size());
        for (String role : roles) {
            buffer.writeVUTF(role);
        }

        return buffer.toByteArray();
    }

    public static AuthorizationContext deserialiaze(byte[] data) {
        DataInput input = new DataInputImpl(data);

        String name = input.readVUTF();
        String tenant = input.readVUTF();

        Set<String> roles = new HashSet<String>();
        int roleCnt = input.readVInt();
        for (int i = 0; i < roleCnt; i++) {
            roles.add(input.readVUTF());
        }

        return new AuthorizationContext(name, tenant, roles);
    }
}
