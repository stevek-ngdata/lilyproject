/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.repository.api.filter;

import org.lilyproject.repository.api.QName;

/**
 * Filters on the record type of records. You can search for exact matches or search for records that
 * are "instance of" ("is a") the given record type, i.e. records which are either of the given type, or one
 * of the ancestor records from which it extends is this type.
 *
 * <p>It is the record type from the non-versioned scope based on which the filtering is performed.</p>
 */
public class RecordTypeFilter implements RecordFilter {
    private QName recordType;
    private Operator operator;
    /** version is only supported when operator == Operator.EQUALS */
    private Long version;

    public static enum Operator {
        EQUALS,
        INSTANCE_OF
    }

    public RecordTypeFilter() {
    }
    
    public RecordTypeFilter(QName recordType) {
        this.recordType = recordType;
    }

    public RecordTypeFilter(QName recordType, Operator operator) {
        this.recordType = recordType;
        this.operator = operator;
    }

    /**
     * Creates a record type filter that searches for record that are of exactly the given record
     * type and version.
     */
    public RecordTypeFilter(QName recordType, Long version) {
        this.recordType = recordType;
        this.version = version;
    }

    public QName getRecordType() {
        return recordType;
    }

    public void setRecordType(QName recordType) {
        this.recordType = recordType;
    }

    /**
     * Get the version of the record type. This is optional, thus can be null.
     */
    public Long getVersion() {
        return version;
    }

    /**
     * Set the version of the record type. This is optional, thus can be null.
     */
    public void setVersion(Long version) {
        this.version = version;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }
}
