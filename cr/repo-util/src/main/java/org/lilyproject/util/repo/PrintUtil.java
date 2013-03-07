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
package org.lilyproject.util.repo;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.Pair;

/**
 * Utilities for producing a readable dump of a Record and a RecordType.
 */
public class PrintUtil {
    public static void print(Record record, Repository repository) {
        print(record, repository, System.out);
    }

    public static void print(Record record, Repository repository, PrintStream out) {
        TypeManager typeManager = repository.getTypeManager();

        // Group the fields per scope
        Map<Scope, Map<QName, Object>> fieldsByScope = new EnumMap<Scope, Map<QName, Object>>(Scope.class);
        Map<QName, Object> undeterminedFields = new TreeMap<QName, Object>(QNAME_COMP);

        for (Scope scope : Scope.values()) {
            fieldsByScope.put(scope, new TreeMap<QName, Object>(QNAME_COMP));
        }

        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            FieldType fieldType = null;
            try {
                fieldType = typeManager.getFieldTypeByName(field.getKey());
            } catch (Throwable t) {
                // field type failed to load, ignore
            }

            if (fieldType != null) {
                fieldsByScope.get(fieldType.getScope()).put(field.getKey(), field.getValue());
            } else {
                undeterminedFields.put(field.getKey(), field.getValue());
            }
        }

        // Produce the printout
        out.println("ID = " + record.getId());
        out.println("Version = " + record.getVersion());

        // We always print out the non-versioned scope, to show its record type
        out.println("Non-versioned scope:");
        printRecordType(record, Scope.NON_VERSIONED, out);
        printFields(fieldsByScope.get(Scope.NON_VERSIONED), record, out);

        if (fieldsByScope.get(Scope.VERSIONED).size() > 0) {
            out.println("Versioned scope:");
            printRecordType(record, Scope.VERSIONED, out);
            printFields(fieldsByScope.get(Scope.VERSIONED), record, out);
        }

        if (fieldsByScope.get(Scope.VERSIONED_MUTABLE).size() > 0) {
            out.println("Versioned-mutable scope:");
            printRecordType(record, Scope.VERSIONED_MUTABLE, out);
            printFields(fieldsByScope.get(Scope.VERSIONED_MUTABLE), record, out);
        }

        if (undeterminedFields.size() > 0) {
            out.println("Fields of which the field type was not found:");
            printFields(undeterminedFields, record, out);
        }

    }

    private static void printFields(Map<QName, Object> fields, Record record, PrintStream out) {
        for (Map.Entry<QName, Object> field : fields.entrySet()) {
            printField(out, 2, field.getKey(), field.getValue());

            Metadata metadata = record.getMetadata(field.getKey());
            if (metadata != null && metadata.getMap().size() > 0) {
                println(out, 4, "metadata:");
                for (Map.Entry<String, Object> entry : metadata.getMap().entrySet()) {
                    println(out, 6, entry.getKey() + " = " + entry.getValue());
                }
            }
        }
    }

    private static void printField(PrintStream out, int indent, QName fieldName, Object fieldValue) {
        print(out, indent, fieldName + " = ");
        printFieldValue(out, indent, fieldValue);
    }

    private static void printFieldValue(PrintStream out, int indent, Object fieldValue) {
        if (fieldValue instanceof List) {
            out.println();
            printListValue(out, indent + 2, (List)fieldValue);
        } else if (fieldValue instanceof Record) {
            out.println();
            printRecordValue(out, indent + 2, (Record)fieldValue);
        } else {
            out.println(fieldValue);
        }
    }

    private static void printListValue(PrintStream out, int indent, List values) {
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            print(out, indent, "[" + i + "] ");
            if (value instanceof List) {
                out.println();
                printListValue(out, indent + 2, (List)value);
            } else {
                printFieldValue(out, indent, value);
            }
        }
    }

    private static void printRecordValue(PrintStream out, int indent, Record record) {
        println(out, indent, "Record of type " + record.getRecordTypeName() + ", version " + record.getRecordTypeVersion());
        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            printField(out, indent, field.getKey(), field.getValue());
        }
    }

    private static void printRecordType(Record record, Scope scope, PrintStream out) {
        out.println("  Record type = " + record.getRecordTypeName(scope) + ", version " + record.getRecordTypeVersion(scope));
    }

    public static void print(RecordType recordType, Repository repository) {
        print(recordType, repository, System.out);
    }

    public static void print(RecordType recordType, Repository repository, PrintStream out) {

        List<SchemaId> failedFieldTypes = new ArrayList<SchemaId>();

        Map<Scope, List<Pair<FieldTypeEntry, FieldType>>> fieldTypeEntriesByScope =
                new EnumMap<Scope, List<Pair<FieldTypeEntry, FieldType>>>(Scope.class);
        for (Scope scope : Scope.values()) {
            fieldTypeEntriesByScope.put(scope, new ArrayList<Pair<FieldTypeEntry, FieldType>>());
        }

        for (FieldTypeEntry fte : recordType.getFieldTypeEntries()) {
            FieldType fieldType = null;
            try {
                fieldType = repository.getTypeManager().getFieldTypeById(fte.getFieldTypeId());
            } catch (Throwable t) {
                // field type failed to load
            }

            if (fieldType != null) {
                fieldTypeEntriesByScope.get(fieldType.getScope()).add(new Pair<FieldTypeEntry, FieldType>(fte, fieldType));
            } else {
                failedFieldTypes.add(fte.getFieldTypeId());
            }
        }


        int indent = 0;

        println(out, indent, "Name = " + recordType.getName());
        println(out, indent, "ID = " + recordType.getId());
        println(out, indent, "Version = " + recordType.getVersion());
        println(out, indent, "Fields:");

        indent += 2;

        if (fieldTypeEntriesByScope.get(Scope.NON_VERSIONED).size() > 0) {
            println(out, indent, "Non-versioned scope:");
            indent += 2;
            printFieldTypes(fieldTypeEntriesByScope.get(Scope.NON_VERSIONED), out, indent);
            indent -= 2;
        }

        if (fieldTypeEntriesByScope.get(Scope.VERSIONED).size() > 0) {
            println(out, indent, "Versioned scope:");
            indent += 2;
            printFieldTypes(fieldTypeEntriesByScope.get(Scope.VERSIONED), out, indent);
            indent -= 2;
        }

        if (fieldTypeEntriesByScope.get(Scope.VERSIONED_MUTABLE).size() > 0) {
            println(out, indent, "Versioned-mutable scope:");
            indent += 2;
            printFieldTypes(fieldTypeEntriesByScope.get(Scope.VERSIONED_MUTABLE), out, indent);
            indent -= 2;
        }

        if (failedFieldTypes.size() > 0) {
            println(out, indent, "Field types that could not be loaded:");
            indent += 2;
            for (SchemaId id : failedFieldTypes) {
                println(out, indent, id.toString());
            }
            indent -= 2;
        }
    }

    private static void printFieldTypes(List<Pair<FieldTypeEntry, FieldType>> fieldTypeEntries, PrintStream out,
            int indent) {

        Collections.sort(fieldTypeEntries, FT_COMP);
        for (Pair<FieldTypeEntry, FieldType> pair : fieldTypeEntries) {
            println(out, indent, "Field");
            indent += 2;
            printFieldType(pair, out, indent);
            indent -= 2;
        }
    }

    private static void printFieldType(Pair<FieldTypeEntry, FieldType> pair, PrintStream out, int indent) {
        FieldTypeEntry fieldTypeEntry = pair.getV1();
        FieldType fieldType = pair.getV2();

        println(out, indent, "Name = " + fieldType.getName());
        println(out, indent, "ID = " + fieldType.getId());
        println(out, indent, "Mandatory = " + fieldTypeEntry.isMandatory());
        try {
            println(out, indent, "ValueType = " + fieldType.getValueType().getName());
        } catch (Throwable t) {
            // value type failed to load
        }
    }

    private static void println(PrintStream out, int indent, String text) {
        StringBuilder buffer = new StringBuilder(indent);
        for (int i = 0; i < indent; i++) {
            buffer.append(" ");
        }

        out.println(buffer + text);
    }

    private static void print(PrintStream out, int indent, String text) {
        StringBuilder buffer = new StringBuilder(indent);
        for (int i = 0; i < indent; i++) {
            buffer.append(" ");
        }

        out.print(buffer + text);
    }

    private static QNameComparator QNAME_COMP = new QNameComparator();

    private static class QNameComparator implements Comparator<QName> {
        @Override
        public int compare(QName o1, QName o2) {
            int cmp = o1.getNamespace().compareTo(o2.getNamespace());
            return cmp == 0 ? o1.getName().compareTo(o2.getName()) : cmp;
        }
    }

    private static FieldTypeByQNameComparator FT_COMP = new FieldTypeByQNameComparator();

    private static class FieldTypeByQNameComparator implements Comparator<Pair<FieldTypeEntry, FieldType>> {
        @Override
        public int compare(Pair<FieldTypeEntry, FieldType> o1, Pair<FieldTypeEntry, FieldType> o2) {
            return QNAME_COMP.compare(o1.getV2().getName(), o2.getV2().getName());
        }
    }
}
