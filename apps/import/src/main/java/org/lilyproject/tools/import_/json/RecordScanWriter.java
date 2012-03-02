package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;

public class RecordScanWriter {
    public static final RecordScanWriter INSTANCE = new RecordScanWriter();

    /**
     *
     * @param repository in case you're wondering: this is used in case there would be a Record-type field
     *                   value, for which the json serialization code uses the repository to look
     *                   up the value type.
     */
    public byte[] toJsonBytes(RecordScan entity, Repository repository) {
        RecordScanJsonMapper.LOCAL_REPOSITORY.set(repository);
        try {
            return RecordScanJsonMapper.INSTANCE.getMapper().writeValueAsBytes(entity);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            RecordScanJsonMapper.LOCAL_REPOSITORY.set(null);
        }
    }

    public String toJsonString(RecordScan entity, Repository repository) {
        RecordScanJsonMapper.LOCAL_REPOSITORY.set(repository);
        try {
            return RecordScanJsonMapper.INSTANCE.getMapper().writeValueAsString(entity);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            RecordScanJsonMapper.LOCAL_REPOSITORY.set(null);
        }
    }
}
