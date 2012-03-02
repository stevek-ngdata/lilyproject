package org.lilyproject.tools.import_.json;

import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;

public class RecordFilterWriter {
    public static final RecordFilterWriter INSTANCE = new RecordFilterWriter();

    public byte[] toJsonBytes(RecordFilter entity, Repository repository) throws RepositoryException, InterruptedException {
        RecordScanJsonMapper.LOCAL_REPOSITORY.set(repository);
        try {
            return RecordScanJsonMapper.INSTANCE.getMapper().writeValueAsBytes(entity);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            RecordScanJsonMapper.LOCAL_REPOSITORY.set(null);
        }
    }
}
