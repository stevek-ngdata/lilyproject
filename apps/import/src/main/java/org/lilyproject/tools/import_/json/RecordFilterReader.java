package org.lilyproject.tools.import_.json;

import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.filter.RecordFilter;

import java.io.IOException;

public class RecordFilterReader {
    public static final RecordScanReader INSTANCE = new RecordScanReader();

    public RecordFilter fromJsonBytes(byte[] data, Repository repository) {
        RecordScanJsonMapper.LOCAL_REPOSITORY.set(repository);
        try {
            return RecordScanJsonMapper.INSTANCE.getMapper().readValue(data, RecordFilter.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            RecordScanJsonMapper.LOCAL_REPOSITORY.set(null);
        }
    }
}
