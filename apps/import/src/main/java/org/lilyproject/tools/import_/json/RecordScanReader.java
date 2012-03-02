package org.lilyproject.tools.import_.json;

import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;

import java.io.IOException;

public class RecordScanReader {
    public static final RecordScanReader INSTANCE = new RecordScanReader();

    public RecordScan fromJsonBytes(byte[] data, Repository repository) {
        RecordScanJsonMapper.LOCAL_REPOSITORY.set(repository);
        try {
            return RecordScanJsonMapper.INSTANCE.getMapper().readValue(data, RecordScan.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            RecordScanJsonMapper.LOCAL_REPOSITORY.set(null);
        }
    }

    public RecordScan fromJsonString(String data, Repository repository) {
        RecordScanJsonMapper.LOCAL_REPOSITORY.set(repository);
        try {
            return RecordScanJsonMapper.INSTANCE.getMapper().readValue(data, RecordScan.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            RecordScanJsonMapper.LOCAL_REPOSITORY.set(null);
        }
    }
}
