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

import java.util.HashMap;
import java.util.Map;

/**
 * Thrown by a {@link ValueType} implementation in case the encoding version of a value
 * is not supported. Some value types encode as part of the value a version number to
 * allow for future evolutions in the encoding. This exception will typically occur
 * when running older code against a newer data.
 */
public class UnknownValueTypeEncodingException extends TypeException {

    private final String valueType;
    private final String encodingVersion;

    public UnknownValueTypeEncodingException(String valueType, byte encodingVersion) {
        this.valueType = valueType;
        this.encodingVersion = String.valueOf((int)encodingVersion);
    }

    public UnknownValueTypeEncodingException(String message, Map<String, String> state) {
        this.valueType = state.get("valueType");
        this.encodingVersion = state.get("encoding");
    }

    @Override
    public String getMessage() {
        return "Unknown encoding '" + encodingVersion + "' encountered for a field of value type '" + valueType + "'";
    }

    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("valueType", valueType);
        state.put("encoding", encodingVersion);
        return state;
    }
}
