/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.repository.impl.valuetype;

import java.util.HashSet;
import java.util.Set;

import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ValueType;

public abstract class AbstractValueType implements ValueType {

    @Override
    public <T> T read(byte[] data) throws RepositoryException, InterruptedException {
        return read(new DataInputImpl(data));
    }

    @Override
    public abstract void write(Object value, DataOutput dataOutput, IdentityRecordStack parentRecords)
            throws RepositoryException, InterruptedException;

    @Override
    public abstract String getBaseName();

    @Override
    public abstract ValueType getDeepestValueType();

    @Override
    public ValueType getNestedValueType() {
        return null;
    }

    @Override
    public byte[] toBytes(Object value, IdentityRecordStack parentRecords) throws RepositoryException,
            InterruptedException {
        DataOutput dataOutput = new DataOutputImpl();
        write(value, dataOutput, parentRecords);
        return dataOutput.toByteArray();
    }

    @Override
    public String getName() {
        return getBaseName();
    }

    @Override
    public int getNestingLevel() {
        return 0;
    }

    @Override
    public Set<Object> getValues(Object value) {
        Set<Object> result = new HashSet<Object>();
        result.add(value);
        return result;
    }

    @Override
    public boolean isMultiValue() {
        return false;
    }

    @Override
    public boolean isHierarchical() {
        return false;
    }
}
