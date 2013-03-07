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
package org.lilyproject.repository.api;



public class BlobReference {


    private Blob blob;
    private RecordId recordId;
    private FieldType fieldType;

    public BlobReference(Blob blob, RecordId recordId, FieldType fieldType) {
        this.blob = blob;
        this.recordId = recordId;
        this.fieldType = fieldType;
    }

    public Blob getBlob() {
        return blob;
    }

    public void setBlob(Blob blob) {
        this.blob = blob;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType= fieldType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((blob == null) ? 0 : blob.hashCode());
        result = prime * result + ((fieldType == null) ? 0 : fieldType.hashCode());
        result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BlobReference other = (BlobReference) obj;
        if (blob == null) {
            if (other.blob != null)
                return false;
        } else if (!blob.equals(other.blob))
            return false;
        if (fieldType == null) {
            if (other.fieldType != null)
                return false;
        } else if (!fieldType.equals(other.fieldType))
            return false;
        if (recordId == null) {
            if (other.recordId != null)
                return false;
        } else if (!recordId.equals(other.recordId))
            return false;
        return true;
    }

}
