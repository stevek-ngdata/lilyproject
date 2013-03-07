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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.lilyproject.util.ArgumentValidator;

/**
 * Specifies what fields to return in a Record object.
 *
 * <p>This can be a manual enumeration of fields, or it can be ALL or NONE. Note that
 * ALL is usually the same as not specifying a ReturnFields object.
 *
 * <p>For ALL and NONE, you can avoid instantiation by using {@link #ALL} and {@link #NONE}</p>
 *
 * <p>Instances of this class are immutable.</p>
 */
public class ReturnFields {
    private List<QName> fields;
    private Type type = Type.ALL;

    public static ReturnFields ALL = new ReturnFields(Type.ALL);
    public static ReturnFields NONE = new ReturnFields(Type.NONE);

    public enum Type {
        ALL, NONE, ENUM
    }

    public ReturnFields() {

    }

    public ReturnFields(Type type) {
        this.type = type;
    }

    public ReturnFields(List<QName> fields) {
        ArgumentValidator.notNull(fields, "fields");
        this.fields = Collections.unmodifiableList(fields);
        this.type = Type.ENUM;
    }

    public ReturnFields(QName... fields) {
        this.fields = Collections.unmodifiableList(Arrays.asList(fields));
        this.type = Type.ENUM;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * This method will only return non-null if {@link #getType()} returns {@link Type#ENUM}.
     */
    public List<QName> getFields() {
        return fields;
    }

    public void setFields(List<QName> fields) {
        this.fields = fields;
    }
}
