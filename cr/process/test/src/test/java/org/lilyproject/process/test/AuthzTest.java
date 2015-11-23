package org.lilyproject.process.test;

import com.google.common.collect.Sets;
import com.ngdata.lily.security.hbase.client.AuthorizationContext;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.junit.Test;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.spi.AuthorizationContextHolder;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

public class AuthzTest {
    /**
     * Verifies the authorization context is properly passed on to HBase.
     */
    @Test
    public void testAuthContextPassOn() throws Exception {
        LilyProxy lilyProxy = new LilyProxy();
        System.setProperty("lily.test.hbase.coprocessor.region.classes", AuthzRegionObserver.class.getName());
        lilyProxy.start();

        // Set the authorization context
        setUser("jules", Sets.newHashSet("engineering"));

        // In-VM access to the repository manager (not going over the RPC interface)
        RepositoryManager repositoryManager = (RepositoryManager)lilyProxy.getLilyServerProxy()
                .getLilyServerTestingUtility().getRuntime().getJavaServiceManager()
                .getService(org.lilyproject.repository.api.RepositoryManager.class);

        // Create a record type
        LRepository repository = repositoryManager.getDefaultRepository();
        TypeManager typeManager = repository.getTypeManager();
        typeManager.recordTypeBuilder()
                .defaultNamespace("authztest")
                .name("Type1")
                .fieldEntry().defineField().name("field1").create().add()
                .fieldEntry().defineField().name("field2").create().add()
                .create();

        assertNull(AuthzRegionObserver.lastSeenAuthzCtx);

        // Create a record
        LTable table = repository.getDefaultTable();
        Record record = table.recordBuilder()
                .defaultNamespace("authztest")
                .recordType("Type1")
                .field("field1", "value 1")
                .field("field2", "value 2")
                .create();

        assertNotNull(AuthzRegionObserver.lastSeenAuthzCtx);
        assertEquals("jules", AuthzRegionObserver.lastSeenAuthzCtx.getName());

        // Read the record with a different user
        setUser("jef", Sets.newHashSet("marketing"));
        table.read(record.getId());
        assertEquals("jef", AuthzRegionObserver.lastSeenAuthzCtx.getName());
    }

    private void setUser(String name, Set<String> roles) {
        AuthorizationContext authzContext = new AuthorizationContext(name, "default", roles);
        AuthorizationContextHolder.setCurrentContext(authzContext);
    }

    public static class AuthzRegionObserver extends BaseRegionObserver {
        public static AuthorizationContext lastSeenAuthzCtx;

        @Override
        public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results)
                throws IOException {
            extractAuthzCtx(get);
        }

        @Override
        public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
                throws IOException {
            extractAuthzCtx(put);
        }

        @Override
        public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,byte[] family, byte[] qualifier,
                                      CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result)
                throws IOException {
            extractAuthzCtx(put);
            return result;
        }

        private void extractAuthzCtx(OperationWithAttributes op) {
            byte[] authzCtxBytes = op.getAttribute(AuthorizationContext.OPERATION_ATTRIBUTE);
            if (authzCtxBytes == null) {
                lastSeenAuthzCtx = null;
            } else {
                lastSeenAuthzCtx = AuthorizationContext.deserialiaze(authzCtxBytes);
            }
        }
    }
}
