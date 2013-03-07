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
package org.lilyproject.tools.tester;

import org.lilyproject.repository.api.RecordId;

public class TestRecord {
    private RecordId recordId;
    private boolean deleted;
    private TestRecordType recordType;

    public TestRecord(RecordId recordId, TestRecordType recordType) {
        this.recordId = recordId;
        this.recordType = recordType;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public TestRecordType getRecordType() {
        return recordType;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TestRecord other = (TestRecord) obj;
        if (recordId == null) {
            if (other.recordId != null) {
                return false;
            }
        } else if (!recordId.equals(other.recordId)) {
            return false;
        }
        return true;
    }
}
