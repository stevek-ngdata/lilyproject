package org.lilyproject.rest;

import java.util.concurrent.TimeUnit;

import org.lilyproject.repository.api.RecordScanner;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

// Convenience class for creating recordscanner caches. For use in the spring application context
public class RecordScannerMapBuilder {
    
    public static Cache<String,RecordScanner> createRecordScannerMap (int delay) {
        return createRecordScannerMap(delay, TimeUnit.SECONDS);
    }
    
    public static Cache<String,RecordScanner> createRecordScannerMap (int delay, TimeUnit unit) {
        Cache<String,RecordScanner> cache = CacheBuilder.newBuilder()
                .expireAfterAccess(delay,  unit)
                .build();
        
        return cache;
    }
}
