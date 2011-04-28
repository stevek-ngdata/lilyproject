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
package org.lilyproject.repository.api;

import java.util.HashMap;
import java.util.Map;

/**
 * Thrown when trying to create or update a field type or record type with a QName which is already used by another field type or record type.
 */
public class ConcurrentUpdateTypeException extends TypeException {
    private String type;

    public ConcurrentUpdateTypeException(String message, Map<String, String> state) {
        this.type = state.get("type");
    }
    
    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("type", type);
        return state;
    }
    
    public ConcurrentUpdateTypeException(String typeName) {
        if (type != null)
            this.type = typeName;
    }

    @Override
    public String getMessage() {
        StringBuilder message = new StringBuilder();
        message.append("Concurrent create or update occurred for field or record type '").append(type).append("'");
        return message.toString();
    }
}
