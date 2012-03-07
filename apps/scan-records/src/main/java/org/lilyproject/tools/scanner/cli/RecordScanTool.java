package org.lilyproject.tools.scanner.cli;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.tools.import_.json.RecordFilterReader;
import org.lilyproject.tools.import_.json.RecordScanReader;
import org.lilyproject.util.repo.PrintUtil;

public class RecordScanTool {
    private Repository repository;
    private static int DEFAULT_CACHE = 1024;
    private static boolean DEFAULT_CACHE_BLOCKS = false;

    public static void count(Repository repository) throws Exception {
        count(repository, null, null, null);
    }
    
    public static void count(Repository repository, String startId, String stopId) throws Exception {
        count(repository, startId, stopId, null);
    }
    
    public static void count(Repository repository, String startId, String stopId, File configFile) throws Exception {
        new RecordScanTool(repository).count(startId, stopId, configFile);
    }
    
    public static void print(Repository repository) throws Exception {
        print(repository, -1l);
    }

    public static void print(Repository repository, long limit) throws Exception {
        print(repository, limit, null);
    }
    
    public static void print(Repository repository, long limit, File config) throws Exception {
        print(repository, null, null, limit, config);
    }
    
    public static void print(Repository repository, String startId, String stopId, long limit, File config) throws Exception {
        new RecordScanTool(repository).print(startId, stopId, limit, config);
    }

    public RecordScanTool(Repository repository) {
        this.repository = repository;
    }

    public void count(String startId, String stopId, File filterFile) throws Exception {
        System.out.println("Counting records");
        RecordScan scan = createRecordScan(startId, stopId, filterFile);
        // We don't need to return fields for counting
        scan.setReturnFields(ReturnFields.NONE);
        RecordScanner scanner = repository.getScanner(scan);
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

    public void print(String startId, String stopId, long limit, File configFile) throws Exception {
        if (limit < 0) {
            limit = Long.MAX_VALUE;
        }

        RecordScan scan = createRecordScan(startId, stopId, configFile);
        RecordScanner scanner = repository.getScanner(scan);
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

    private RecordScan createRecordScan(String startId, String stopId, File scanConfFile) throws Exception {
        RecordScan scan = new RecordScan();
        scan.setCaching(RecordScanTool.DEFAULT_CACHE);
        scan.setCacheBlocks(DEFAULT_CACHE_BLOCKS);
        
        if (scanConfFile != null) {
            RecordScanReader scanReader = new RecordScanReader();
            scan = scanReader.fromJsonBytes(IOUtils.toByteArray(new FileInputStream(scanConfFile)), repository);    
        }
        
        if (startId != null && startId.length() > 0) {
            scan.setStartRecordId(repository.getIdGenerator().fromString(startId));
        }
        
        if (stopId != null && stopId.length() > 0) {
            scan.setStopRecordId(repository.getIdGenerator().fromString(stopId));
        }
        

        return scan;
    }
}
