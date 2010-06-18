package org.lilycms.testfw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides access to HBase, either by starting an embedded HBase or by connecting to a running HBase.
 *
 * <p>This is intended for usage in test cases.
 *
 * <p><b>VERY VERY IMPORTANT</b>: when connecting to an existing HBase, this class will DELETE ALL ROWS
 * FROM ALL TABLES!
 *
 * <p>For Lily, the mode where we connect to an external HBase currently does not work very well,
 * especially those parts that make use of versioned column families with sequence-number timestamps,
 * since the deletes performed on those table mask the new inserts. See also http://search-hadoop.com/m/rNnhN15Xecu
 */
public class HBaseProxy {
    private static Mode MODE;
    private static Configuration CONF;
    private static HBaseTestingUtility TEST_UTIL;

    private enum Mode { EMBED, CONNECT }
    private static String HBASE_MODE_PROP_NAME = "lily.test.hbase";

    private static Set<String> RETAIN_TABLES = new HashSet<String>();
    static {
        RETAIN_TABLES.add("indexmeta");
    }

    public void start() throws Exception {
        String hbaseModeProp = System.getProperty(HBASE_MODE_PROP_NAME);
        if (hbaseModeProp == null || hbaseModeProp.equals("") || hbaseModeProp.equals("embed")) {
            MODE = Mode.EMBED;
        } else if (hbaseModeProp.equals("connect")) {
            MODE = Mode.CONNECT;
        } else {
            throw new RuntimeException("Unexpected value for " + HBASE_MODE_PROP_NAME + ": " + hbaseModeProp);
        }

        System.out.println("HBase usage mode: " + MODE);

        switch (MODE) {
            case EMBED:
                // TODO use optimized hbase config
                TEST_UTIL = new HBaseTestingUtility();
                TEST_UTIL.startMiniCluster(1);
                CONF = TEST_UTIL.getConfiguration();
                break;
            case CONNECT:
                CONF = HBaseConfiguration.create();
                CONF.set("hbase.zookeeper.quorum", "localhost");
                CONF.set("hbase.zookeeper.property.clientPort", "21812"); // matches HBaseRunner
                cleanTables();
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + MODE);
        }
    }

    public void stop() throws Exception {
        if (MODE == Mode.EMBED) {
            TEST_UTIL.shutdownMiniCluster();
            TEST_UTIL = null;
        }
        CONF = null;
    }

    public Configuration getConf() {
        return CONF;
    }

    public FileSystem getBlobFS() throws IOException, URISyntaxException {
        if (MODE == Mode.EMBED) {
            return TEST_UTIL.getDFSCluster().getFileSystem();
        } else {
            return FileSystem.get(new URI("hdfs://localhost:9000"), getConf());
        }
    }

    private void cleanTables() throws Exception {
        System.out.println("=========================== Resetting HBase tables ===========================");

        StringBuilder truncateReport = new StringBuilder();
        StringBuilder retainReport = new StringBuilder();

        HBaseAdmin admin = new HBaseAdmin(getConf());
        HTableDescriptor[] tables = admin.listTables();
        System.out.println("Found tables: " + tables.length);

        for (HTableDescriptor table : tables) {
            if (RETAIN_TABLES.contains(table.getNameAsString())) {
                if (retainReport.length() > 0)
                    retainReport.append(", ");
                retainReport.append(table.getNameAsString());
                continue;
            }

            HTable htable = new HTable(getConf(), table.getName());

            Scan scan = new Scan();
            ResultScanner scanner = htable.getScanner(scan);
            Result[] results;
            int totalCount = 0;

            while ((results = scanner.next(1000)).length > 0) {
                List<Delete> deletes = new ArrayList<Delete>(results.length);
                for (Result result : results) {
                    deletes.add(new Delete(result.getRow()));
                }
                totalCount += deletes.size();
                htable.delete(deletes);
            }

            if (truncateReport.length() > 0)
                truncateReport.append(", ");
            truncateReport.append(table.getNameAsString()).append(" (").append(totalCount).append(")");

            scanner.close();
            htable.close();
        }

        truncateReport.insert(0, "Truncated the following tables: ");
        retainReport.insert(0, "Did not truncate the following tables: ");

        System.out.println(truncateReport);
        System.out.println(retainReport);

        System.out.println("==============================================================================");

    }
}
