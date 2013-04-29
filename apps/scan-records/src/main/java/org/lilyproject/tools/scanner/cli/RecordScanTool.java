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
package org.lilyproject.tools.scanner.cli;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.repository.api.filter.RecordFilterList;
import org.lilyproject.repository.api.filter.RecordFilterList.Operator;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.tools.import_.json.RecordScanReader;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.repo.PrintUtil;

public class RecordScanTool {
    private Repository repository;
    private LTable table;
    private static int DEFAULT_CACHE = 1024;
    private static boolean DEFAULT_CACHE_BLOCKS = false;

    public static void count(Repository repository, LTable table) throws Exception {
        count(repository, table, null, null);
    }

    public static void count(Repository repository, LTable table, String startId, String stopId) throws Exception {
        count(repository, table, startId, stopId, null, null);
    }

    public static void count(Repository repository, LTable table, String startId, String stopId,
            String recordTypeFilter, File configFile) throws Exception {
        new RecordScanTool(repository, table).count(startId, stopId, recordTypeFilter, configFile);
    }

    public static void print(Repository repository, LTable table) throws Exception {
        print(repository, table, -1L);
    }

    public static void print(Repository repository, LTable table, long limit) throws Exception {
        print(repository, table, limit, null);
    }

    public static void print(Repository repository, LTable table, long limit, File config) throws Exception {
        print(repository, table, null, null, limit, null, config);
    }

    public static void print(Repository repository, LTable table, String startId, String stopId, long limit,
            String recordTypeFilter, File config) throws Exception {
        new RecordScanTool(repository, table).print(startId, stopId, limit, recordTypeFilter, config);
    }

    public RecordScanTool(Repository repository, LTable table) {
        this.repository = repository;
        this.table = table;
    }

    public void count(String startId, String stopId, String recordTypeFilter, File configFile) throws Exception {
        System.out.println("Counting records");
        RecordScan scan = createRecordScan(startId, stopId, recordTypeFilter, configFile);
        if (configFile == null) {
            // We don't need to return fields for counting
            scan.setReturnFields(ReturnFields.NONE);
        }
        RecordScanner scanner = table.getScanner(scan);
        Record record;
        Date start = new Date();
        try {
            int i = 0;
            while ((record = scanner.next()) != null) {
                i++;
                if (i % 1000 == 0) {
                    System.out.println("Record no° : " + i + ", Record id : " + record.getId() );
                }
            }
            Date stop = new Date();
            System.out.println("Found " + i + " records in " + ((stop.getTime() - start.getTime()) / 1000f) + "s");
        } finally {
            scanner.close();
        }
    }

    public void print(String startId, String stopId, long limit, String recordTypeFilter, File configFile) throws Exception {
        if (limit < 0) {
            limit = Long.MAX_VALUE;
        }

        RecordScan scan = createRecordScan(startId, stopId, recordTypeFilter, configFile);
        RecordScanner scanner = table.getScanner(scan);
        try {
            int i = 0;
            Record record;
            while ((record = scanner.next()) != null && i < limit) {
                i++;
                PrintUtil.print(record, repository);
            }
        } finally {
            scanner.close();
        }

    }

    private RecordScan createRecordScan(String startId, String stopId, String recordTypeFilter, File scanConfFile) throws Exception {
        RecordScan scan = null;

        if (scanConfFile != null) {
            JsonNode jsonNode = JsonFormat.deserializeNonStd(new FileInputStream(scanConfFile));
            scan = RecordScanReader.INSTANCE.fromJson(jsonNode, repository);
        }

        scan = scan != null ? scan : new RecordScan();
        scan.setCaching(RecordScanTool.DEFAULT_CACHE);
        scan.setCacheBlocks(DEFAULT_CACHE_BLOCKS);

        if (startId != null && startId.length() > 0) {
            scan.setStartRecordId(repository.getIdGenerator().fromString(startId));
        }

        if (stopId != null && stopId.length() > 0) {
            scan.setStopRecordId(repository.getIdGenerator().fromString(stopId));
        }

        if (recordTypeFilter != null && !recordTypeFilter.isEmpty()) {
            RecordFilterList filterList = new RecordFilterList(Operator.MUST_PASS_ONE);
            if (scan.getRecordFilter() != null) {
                filterList.addFilter(scan.getRecordFilter());
            }
            scan.setRecordFilter(filterList);

            String[] recordTypes = recordTypeFilter.split(",");
            for (String recordType : recordTypes) {
                filterList.addFilter(new RecordTypeFilter(QName.fromString(recordType)));
            }
        }

        return scan;
    }
}
