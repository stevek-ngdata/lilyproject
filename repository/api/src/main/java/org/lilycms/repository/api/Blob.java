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
package org.lilycms.repository.api;

import java.util.Arrays;

import org.lilycms.util.ArgumentValidator;

/**
 * A Blob 
 *
 */
public class Blob {

    private final String mimetype;
    private final Long size;
    private final String name;
    private byte[] value;

    public Blob(byte[] value, String mimetype, Long size, String name) {
        ArgumentValidator.notNull(mimetype, "mimetype");
        this.value = value;
        this.mimetype = mimetype;
        this.size = size;
        this.name = name;
    }
    
    public Blob(String mimetype, Long size, String name) {
        this(null, mimetype, size, name);
    }
    
    public byte[] getValue() {
        return value;
    }
    
    public void setValue(byte[] value) {
        this.value = value;
    }
    
    public String getMimetype() {
        return mimetype;
    }
    
    public Long getSize() {
        return size;
    }
    
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + Arrays.hashCode(value);
        result = prime * result + ((mimetype == null) ? 0 : mimetype.hashCode());
        result = prime * result + ((size == null) ? 0 : size.hashCode());
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
        Blob other = (Blob) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        if (mimetype == null) {
            if (other.mimetype != null)
                return false;
        } else if (!mimetype.equals(other.mimetype))
            return false;
        if (size == null) {
            if (other.size != null)
                return false;
        } else if (!size.equals(other.size))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Blob [name=" + name + ", mimetype=" + mimetype + ", size=" + size + ", value="
                        + Arrays.toString(value) + "]";
    }
}
