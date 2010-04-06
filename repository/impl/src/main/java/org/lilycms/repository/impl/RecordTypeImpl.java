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
package org.lilycms.repository.impl;

import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.TypeManager;

public class RecordTypeImpl implements RecordType {
    
    private final String id;
    private Long version;
    private String nonVersionableFieldGroupId;
    private String versionableFieldGroupId;
    private String versionableMutableFieldGroupId;
    private Long nonVersionableFieldGroupVersion;
    private Long versionableFieldGroupVersion;
    private Long versionableMutableFieldGroupVersion;

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newRecordType} instead
     */
    public RecordTypeImpl(String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version){
        this.version = version;
    }

    public String getNonVersionableFieldGroupId() {
        return nonVersionableFieldGroupId;
    }

    public Long getNonVersionableFieldGroupVersion() {
        return nonVersionableFieldGroupVersion;
    }

    public String getVersionableFieldGroupId() {
        return versionableFieldGroupId;
    }


    public Long getVersionableFieldGroupVersion() {
        return versionableFieldGroupVersion;
    }

    public String getVersionableMutableFieldGroupId() {
        return versionableMutableFieldGroupId;
    }

    public Long getVersionableMutableFieldGroupVersion() {
        return versionableMutableFieldGroupVersion;
    }

    public void setNonVersionableFieldGroupId(String id) {
        nonVersionableFieldGroupId = id;
    }

    public void setNonVersionableFieldGroupVersion(Long version) {
        nonVersionableFieldGroupVersion = version;
    }

    public void setVersionableFieldGroupId(String id) {
        versionableFieldGroupId = id;
    }

    public void setVersionableFieldGroupVersion(Long version) {
        versionableFieldGroupVersion = version;
    }

    public void setVersionableMutableFieldGroupId(String id) {
        versionableMutableFieldGroupId = id;
    }
    
    public void setVersionableMutableFieldGroupVersion(Long version) {
        versionableMutableFieldGroupVersion = version;
    }

    public RecordType clone() {
        RecordTypeImpl clone = new RecordTypeImpl(this.id);
        clone.version = this.version;
        clone.nonVersionableFieldGroupId = this.nonVersionableFieldGroupId;
        clone.nonVersionableFieldGroupVersion = this.nonVersionableFieldGroupVersion;
        clone.versionableFieldGroupId = this.versionableFieldGroupId;
        clone.versionableFieldGroupVersion = this.versionableFieldGroupVersion;
        clone.versionableMutableFieldGroupId = this.versionableMutableFieldGroupId;
        clone.versionableMutableFieldGroupVersion = this.versionableMutableFieldGroupVersion;
        return clone;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((nonVersionableFieldGroupId == null) ? 0 : nonVersionableFieldGroupId.hashCode());
        result = prime * result
                        + ((nonVersionableFieldGroupVersion == null) ? 0 : nonVersionableFieldGroupVersion.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        result = prime * result + ((versionableFieldGroupId == null) ? 0 : versionableFieldGroupId.hashCode());
        result = prime * result
                        + ((versionableFieldGroupVersion == null) ? 0 : versionableFieldGroupVersion.hashCode());
        result = prime * result
                        + ((versionableMutableFieldGroupId == null) ? 0 : versionableMutableFieldGroupId.hashCode());
        result = prime
                        * result
                        + ((versionableMutableFieldGroupVersion == null) ? 0 : versionableMutableFieldGroupVersion
                                        .hashCode());
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
        RecordTypeImpl other = (RecordTypeImpl) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (nonVersionableFieldGroupId == null) {
            if (other.nonVersionableFieldGroupId != null)
                return false;
        } else if (!nonVersionableFieldGroupId.equals(other.nonVersionableFieldGroupId))
            return false;
        if (nonVersionableFieldGroupVersion == null) {
            if (other.nonVersionableFieldGroupVersion != null)
                return false;
        } else if (!nonVersionableFieldGroupVersion.equals(other.nonVersionableFieldGroupVersion))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        if (versionableFieldGroupId == null) {
            if (other.versionableFieldGroupId != null)
                return false;
        } else if (!versionableFieldGroupId.equals(other.versionableFieldGroupId))
            return false;
        if (versionableFieldGroupVersion == null) {
            if (other.versionableFieldGroupVersion != null)
                return false;
        } else if (!versionableFieldGroupVersion.equals(other.versionableFieldGroupVersion))
            return false;
        if (versionableMutableFieldGroupId == null) {
            if (other.versionableMutableFieldGroupId != null)
                return false;
        } else if (!versionableMutableFieldGroupId.equals(other.versionableMutableFieldGroupId))
            return false;
        if (versionableMutableFieldGroupVersion == null) {
            if (other.versionableMutableFieldGroupVersion != null)
                return false;
        } else if (!versionableMutableFieldGroupVersion.equals(other.versionableMutableFieldGroupVersion))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RecordTypeImpl [id=" + id + ", version=" + version + ", nonVersionableFieldGroupId="
                        + nonVersionableFieldGroupId + ", nonVersionableFieldGroupVersion="
                        + nonVersionableFieldGroupVersion + ", versionableFieldGroupId=" + versionableFieldGroupId
                        + ", versionableFieldGroupVersion=" + versionableFieldGroupVersion
                        + ", versionableMutableFieldGroupId=" + versionableMutableFieldGroupId
                        + ", versionableMutableFieldGroupVersion=" + versionableMutableFieldGroupVersion + "]";
    }
}
