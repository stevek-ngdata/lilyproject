/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.util.hbase;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

public class ColumnFamilyConfig {
    private Compression.Algorithm compression;
    private Integer blockSize;
    private BloomType boomFilter;

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

    public BloomType getBoomFilter() {
        return boomFilter;
    }

    public void setBoomFilter(BloomType boomFilter) {
        this.boomFilter = boomFilter;
    }
}
