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
package org.lilyproject.rest.providers.json;

import java.util.HashMap;
import java.util.Map;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.tools.import_.json.*;

public class EntityRegistry {
    protected static Map<Class, RegistryEntry> SUPPORTED_TYPES;
    static {
        SUPPORTED_TYPES = new HashMap<Class, RegistryEntry>();
        SUPPORTED_TYPES.put(RecordType.class, new RegistryEntry(RecordTypeReader.INSTANCE, RecordTypeWriter.INSTANCE));
        SUPPORTED_TYPES.put(FieldType.class, new RegistryEntry(FieldTypeReader.INSTANCE, FieldTypeWriter.INSTANCE));
        SUPPORTED_TYPES.put(Record.class, new RegistryEntry(RecordReader.INSTANCE, RecordWriter.INSTANCE));
    }

    public static EntityReader findReader(Class clazz) {
        for (Map.Entry<Class, EntityRegistry.RegistryEntry> entry : EntityRegistry.SUPPORTED_TYPES.entrySet()) {
            if (clazz.isAssignableFrom(entry.getKey())) {
                return entry.getValue().getReader();
            }
        }
        throw new RuntimeException("No entity reader for class " + clazz.getName());
    }

    public static RegistryEntry findReaderRegistryEntry(Class clazz) {
        for (Map.Entry<Class, EntityRegistry.RegistryEntry> entry : EntityRegistry.SUPPORTED_TYPES.entrySet()) {
            if (clazz.isAssignableFrom(entry.getKey())) {
                return entry.getValue();
            }
        }
        throw new RuntimeException("No entity reader for class " + clazz.getName());
    }

    public static EntityWriter findWriter(Class clazz) {
        for (Map.Entry<Class, EntityRegistry.RegistryEntry> entry : EntityRegistry.SUPPORTED_TYPES.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return entry.getValue().getWriter();
            }
        }
        throw new RuntimeException("No entity writer for class " + clazz.getName());
    }

    public static class RegistryEntry {
        EntityReader reader;
        EntityWriter writer;

        public RegistryEntry(EntityReader reader, EntityWriter writer) {
            this.reader = reader;
            this.writer = writer;
        }

        public EntityReader getReader() {
            return reader;
        }

        public EntityWriter getWriter() {
            return writer;
        }
    }
}
