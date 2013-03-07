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
package org.lilyproject.server.modules.general;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.runtime.conf.Conf;
import org.lilyproject.util.hbase.ColumnFamilyConfig;
import org.lilyproject.util.hbase.TableConfig;
import org.lilyproject.util.hbase.TableConfigEntry;

public class TableConfigBuilder {
    public static List<TableConfigEntry> buildTableConfigs(Conf conf) {
        List<TableConfigEntry> result = new ArrayList<TableConfigEntry>();

        for (Conf table : conf.getChildren("table")) {
            String tableName = table.getAttribute("name");
            Pattern tableNamePattern;
            try {
                tableNamePattern = Pattern.compile(tableName);
            } catch (PatternSyntaxException e) {
                throw new RuntimeException("The table name should be a valid regular expression, " +
                        "which the following is not: " + tableName + " at " + conf.getLocation());
            }

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

            result.add(new TableConfigEntry(tableNamePattern, config));
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
