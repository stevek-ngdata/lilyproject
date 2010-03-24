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

/**
 * Represents the primitive type that can be used for the value of a field. It
 * should be embeded in a {@link ValueType} before using it in a
 * {@link FieldDescriptor} The implementors of this interface are responsible
 * for the encoding to and decoding from {@code byte[]} of the values of their
 * type.
 * 
 * When a new primitive type should made available, this interface needs to be
 * implemented and registered by calling
 * {@link TypeManager#registerPrimitiveValueType(PrimitiveValueType)}
 */
public interface PrimitiveValueType {
    /**
     * @return a name which is unique over all {@link PrimitiveValueType}s
     */
    String getName();

    /**
     * Decodes a byte[] to an object of the actual type of the
     * {@link PrimitiveValueType}
     */
    public Object fromBytes(byte[] value);

    /**
     * Encodes an object of the actual type of the {@link PrimitiveValueType} to
     * a byte[]
     */
    byte[] toBytes(Object value);

    /**
     * @return the actual type (e.g. {@link String}) that is represented by this
     *         {@link PrimitiveValueType}
     */
    Class getType();
}
