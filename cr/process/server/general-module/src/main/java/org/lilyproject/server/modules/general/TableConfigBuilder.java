package org.lilyproject.server.modules.general;

import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.kauriproject.conf.Conf;
import org.lilyproject.util.ByteArrayKey;
import org.lilyproject.util.hbase.ColumnFamilyConfig;
import org.lilyproject.util.hbase.TableConfig;

import java.util.HashMap;
import java.util.Map;

public class TableConfigBuilder {
    public static Map<ByteArrayKey, TableConfig> buildTableConfigs(Conf conf) {
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

            for (Conf familyConf : table.getChild("families").getChildren("family")) {
                ColumnFamilyConfig family = buildCfConfig(familyConf);
                String familyName = familyConf.getAttribute("name");
                config.getColumnFamilies().put(familyName, family);
            }

            result.put(new ByteArrayKey(tableName), config);
        }

        return result;
    }

    public static ColumnFamilyConfig buildCfConfig(Conf conf) {
        ColumnFamilyConfig family = new ColumnFamilyConfig();

        String compression = conf.getChild("compression").getValue(null);
        if (compression != null) {
            family.setCompression(Compression.Algorithm.valueOf(compression.toUpperCase()));
        }

        String bloomFilter = conf.getChild("bloomFilter").getValue(null);
        if (bloomFilter != null) {
            family.setBoomFilter(StoreFile.BloomType.valueOf(bloomFilter.toUpperCase()));
        }

        Integer blockSize = conf.getChild("blockSize").getValueAsInteger(null);
        if (blockSize != null) {
            family.setBlockSize(blockSize);
        }

        return family;
    }
}
