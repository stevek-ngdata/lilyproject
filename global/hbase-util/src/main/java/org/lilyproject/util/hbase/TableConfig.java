package org.lilyproject.util.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TableConfig {
    private Integer regionCount;
    private String splitKeysAsString;
    private byte[] splitKeyPrefix;
    private Long maxFileSize;
    private Long memStoreFlushSize;
    private Map<String, ColumnFamilyConfig> columnFamilies = new HashMap<String, ColumnFamilyConfig>();

    public TableConfig() {

    }

    /**
     * @param splitKeys (optional, can be null) comma-separated list of split keys. If this is specified, it takes
     *                   precedence over the nrOfRegions parameter.
     * @param regionCount (optional, can be null) number of regions. Creates splits suited for row keys that are
     *                    random UUIDs.
     */
    public TableConfig(Integer regionCount, String splitKeys, byte[] splitKeyPrefix) {
        this.regionCount = regionCount;
        this.splitKeysAsString = splitKeys;
        this.splitKeyPrefix = splitKeyPrefix == null ? new byte[0] : splitKeyPrefix;
    }

    public Map<String, ColumnFamilyConfig> getColumnFamilies() {
        return columnFamilies;
    }

    public ColumnFamilyConfig getColumnFamilyConfig(String family) {
        return columnFamilies.containsKey(family) ? columnFamilies.get(family) : new ColumnFamilyConfig();
    }

    public Long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(Long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public Long getMemStoreFlushSize() {
        return memStoreFlushSize;
    }

    public void setMemStoreFlushSize(Long memStoreFlushSize) {
        this.memStoreFlushSize = memStoreFlushSize;
    }

    public byte[][] getSplitKeys() {
        byte[][] splitKeys = null;
        if (splitKeysAsString != null && !splitKeysAsString.isEmpty()) {
            String[] split = splitKeysAsString.split(",");
            splitKeys = new byte[split.length][];
            for (int i = 0; i < split.length; i++) {
                splitKeys[i] = Bytes.add(splitKeyPrefix, Bytes.toBytesBinary(split[i]));
            }
        } else if (regionCount != null && regionCount <= 1) {
            // one region requested, no need to define splits
        } else if (regionCount != null) {
            byte[] startBytes = new byte[]{(byte)0};
            byte[] endBytes =  new byte[splitKeyPrefix.length + 16];
            System.arraycopy(splitKeyPrefix, 0, endBytes, 0, splitKeyPrefix.length);
            for (int i = splitKeyPrefix.length; i < endBytes.length; i++) {
                endBytes[i] = (byte)0xFF;
            }
            // number of splits = number of regions - 1
            splitKeys = Bytes.split(startBytes, endBytes, regionCount - 1);
            // Stripping the first key to avoid a region [null,0[ which will always be empty
            // And the last key to avoind [xffxffxff....,null[ to contain only few values if variants are created
            // for a record with record id xffxffxff.....
            splitKeys = Arrays.copyOfRange(splitKeys, 1, splitKeys.length - 1);
        }

        return splitKeys;
    }
}
