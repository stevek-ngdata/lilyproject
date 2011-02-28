package org.lilyproject.util.hbase;

import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;

public class ColumnFamilyConfig {
    private Compression.Algorithm compression;
    private Integer blockSize;
    private StoreFile.BloomType boomFilter;

    public Compression.Algorithm getCompression() {
        return compression;
    }

    public void setCompression(Compression.Algorithm compression) {
        this.compression = compression;
    }

    public Integer getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
    }

    public StoreFile.BloomType getBoomFilter() {
        return boomFilter;
    }

    public void setBoomFilter(StoreFile.BloomType boomFilter) {
        this.boomFilter = boomFilter;
    }
}
