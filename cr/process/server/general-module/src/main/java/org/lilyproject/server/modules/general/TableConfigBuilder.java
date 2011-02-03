package org.lilyproject.server.modules.general;

import org.apache.hadoop.hbase.util.Bytes;
import org.kauriproject.conf.Conf;
import org.lilyproject.util.ByteArrayKey;
import org.lilyproject.util.hbase.TableConfig;

import java.util.HashMap;
import java.util.Map;

public class TableConfigBuilder {
    public static Map<ByteArrayKey, TableConfig> build(Conf conf) {
        Map<ByteArrayKey, TableConfig> result = new HashMap<ByteArrayKey, TableConfig>();

        for (Conf table : conf.getChildren("table")) {
            byte[] tableName = Bytes.toBytes(table.getAttribute("name"));

            Integer regionCount = table.getChild("splits").getChild("regionCount").getValueAsInteger(-1);
            String splitKeys = table.getChild("splits").getChild("splitKeys").getValue(null);
            String splitKeyPrefix = table.getChild("splits").getChild("splitKeyPrefix").getValue(null);
            byte[] splitKeyPrefixBytes = splitKeyPrefix != null ? Bytes.toBytesBinary(splitKeyPrefix) : null;

            Long maxFileSize = table.getChild("maxFileSize").getValueAsLong(null);
            Long memStoreFlushSize = table.getChild("memStoreFlushSize").getValueAsLong(null);

            TableConfig config = new TableConfig(regionCount, splitKeys, splitKeyPrefixBytes);
            config.setMaxFileSize(maxFileSize);
            config.setMemStoreFlushSize(memStoreFlushSize);

            result.put(new ByteArrayKey(tableName), config);
        }

        return result;
    }
}
