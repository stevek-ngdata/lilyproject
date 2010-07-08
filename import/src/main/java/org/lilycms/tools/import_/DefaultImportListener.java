package org.lilycms.tools.import_;

import java.io.PrintStream;

public class DefaultImportListener implements ImportListener {
    private PrintStream out;

    public DefaultImportListener() {
        this(System.out);
    }

    public DefaultImportListener(PrintStream out) {
        this.out = out;
    }

    public void conflict(EntityType entityType, String entityName, String propName, Object oldValue, Object newValue)
            throws ImportConflictException {
        throw new ImportConflictException(String.format("%1$s %2$s exists but with %3$s %4$s instead of %5$s",
                toText(entityType), entityName, propName, oldValue, newValue));
    }

    public void existsAndEqual(EntityType entityType, String entityName) {
        out.println(String.format("%1$s already exists and is equal: %2$s", toText(entityType), entityName));
    }

    public void updated(EntityType entityType, String entityName, String entityId) {
        out.println(String.format("%1$s updated: %2$s", toText(entityType), entityName));
    }

    public void created(EntityType entityType, String entityName, String entityId) {
        out.println(String.format("%1$s created: %2$s", toText(entityType), entityName));                        
    }

    private String toText(EntityType entityType) {
        String entityTypeName;
        switch (entityType) {
            case FIELD_TYPE:
                entityTypeName = "Field type";
                break;
            case RECORD_TYPE:
                entityTypeName = "Record type";
                break;
            default:
                throw new RuntimeException("Unexpected entity type: " + entityType);
        }
        return entityTypeName;
    }
}
