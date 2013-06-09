/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.tools.import_.cli;

import java.io.PrintStream;
import java.util.EnumMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.joda.time.DateTime;

public class DefaultImportListener implements ImportListener {
    private PrintStream out;

    private EnumMap<EntityType, AtomicInteger> counters;

    private Set<EntityType> suppressedTypes;

    public DefaultImportListener() {
        this(System.out);
    }

    public DefaultImportListener(PrintStream out, EntityType... suppressedTypes) {
        this.out = out;
        this.suppressedTypes = Sets.newHashSet(suppressedTypes);
        counters = new EnumMap<EntityType, AtomicInteger>(EntityType.class);
        for (EntityType type : EntityType.values()) {
            counters.put(type, new AtomicInteger(0));
        }
    }

    @Override
    public void exception(Throwable throwable) {
        out.println("Error during import:");
        throwable.printStackTrace(out);
    }

    @Override
    public void recordImportException(Throwable throwable, String json, int lineNumber) {
        out.println("Error importing record at line " + lineNumber + ", json: " + json);
        throwable.printStackTrace();
    }

    @Override
    public void tooManyRecordImportErrors(long count) {
        out.println("Encountered " + count + " errors importing records, aborting.");
    }

    @Override
    public void conflict(EntityType entityType, String entityName, String propName, Object oldValue, Object newValue)
            throws ImportConflictException {
        throw new ImportConflictException(String.format("%1$s %2$s exists but with %3$s %4$s instead of %5$s",
                toText(entityType), entityName, propName, oldValue, newValue));
    }

    @Override
    public void existsAndEqual(EntityType entityType, String entityName, String entityId) {
        if (!checkSuppressed(entityType)) {
            out.println(String.format("%1$s already exists and is equal: %2$s", toText(entityType), id(entityName, entityId)));
        }
    }

    @Override
    public void updated(EntityType entityType, String entityName, String entityId, Long version) {
        if (entityType == EntityType.RECORD || entityType == EntityType.RECORD_TYPE) {
            if (!checkSuppressed(entityType)) {
                String versionMsg = version == null ? "non-versioned" : "version " + version;
                out.println(String.format("%1$s updated: %2$s (%3$s)",
                        toText(entityType), id(entityName, entityId), versionMsg));
            }
        } else {
            if (!checkSuppressed(entityType)) {
                out.println(String.format("%1$s updated: %2$s", toText(entityType), id(entityName, entityId)));
            }
        }
    }

    @Override
    public void allowedFailure(EntityType entityType, String entityName, String entityId, String reason) {
        if (!checkSuppressed(entityType)) {
            out.println(String.format("%1$s operation failed: %2$s: %3$s", toText(entityType), id(entityName, entityId),
                    reason));
        }
    }

    private boolean checkSuppressed(EntityType entityType) {
        if (suppressedTypes.contains(entityType)) {
            int count = counters.get(entityType).incrementAndGet();
            if ((count % 1000) == 0) {
                out.print(".");

                if (entityType == EntityType.RECORD) {
                    if ((count % 50000) == 0) {
                        out.println(new DateTime() + " - records imported: " + count);
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void created(EntityType entityType, String entityName, String entityId) {
        if (!checkSuppressed(entityType)) {
            out.println(String.format("%1$s created: %2$s", toText(entityType), id(entityName, entityId)));
        }
    }

    private String id(String entityName, String entityId) {
        if (entityName != null) {
            return entityName;
        } else {
            return entityId.toString();
        }
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
            case RECORD:
                entityTypeName = "Record";
                break;
            default:
                throw new RuntimeException("Unexpected entity type: " + entityType);
        }
        return entityTypeName;
    }
}
