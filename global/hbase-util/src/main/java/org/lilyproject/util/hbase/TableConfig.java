package org.lilyproject.util.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class TableConfig {
    private Integer regionCount;
    private String splitKeysAsString;

    public TableConfig() {

    }

    /**
     * @param splitKeys (optional, can be null) comma-separated list of split keys. If this is specified, it takes
     *                   precedence over the nrOfRegions parameter.
     * @param regionCount (optional, can be null) number of regions. Creates splits suited for row keys that are
     *                    random UUIDs.
     */
    public TableConfig(Integer regionCount, String splitKeys) {
        this.regionCount = regionCount;
        this.splitKeysAsString = splitKeys;
    }

    public byte[][] getSplitKeys() {
        return getSplitKeys(null);
    }

    public byte[][] getSplitKeys(byte[] prefixBytes) {
        if (prefixBytes == null) {
            prefixBytes = new byte[0];
        }

        byte[][] splitKeys = null;
        if (splitKeysAsString != null && !splitKeysAsString.isEmpty()) {
            String[] split = splitKeysAsString.split(",");
            splitKeys = new byte[split.length][];
            for (int i = 0; i < split.length; i++) {
                splitKeys[i] = Bytes.add(prefixBytes, Bytes.toBytes(split[i]));
            }
        } else if (regionCount != null) {
            byte[] startBytes = new byte[]{(byte)0};
            byte[] endBytes =  new byte[prefixBytes.length + 16];
            System.arraycopy(prefixBytes, 0, endBytes, 0, prefixBytes.length);
            for (int i = prefixBytes.length; i < endBytes.length; i++) {
                endBytes[i] = (byte)0xFF;
            }
            byte[][] splits = Bytes.split(startBytes, endBytes, regionCount);
            // Stripping the first key to avoid a region [null,0[ which will always be empty
            // And the last key to avoind [xffxffxff....,null[ to contain only few values if variants are created
            // for a record with record id xffxffxff.....
            splitKeys = Arrays.copyOfRange(splits, 1, splits.length - 1);
        }
        return splitKeys;
    }
}
