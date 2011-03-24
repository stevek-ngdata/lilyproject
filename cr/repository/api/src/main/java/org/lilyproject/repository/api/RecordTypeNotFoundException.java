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

public class RecordTypeNotFoundException extends TypeException {

    private String id;
    private String name;
    private Long version;

    public RecordTypeNotFoundException(String message, Map<String, String> state) {
        this.id = state.get("id");
        this.name = state.get("name");
        String version = state.get("version");
        this.version = version != null ? Long.valueOf(version) : null; 
    }
    
    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("id", id);
        state.put("name", name);
        state.put("version", version != null ? version.toString() : null);
        return state;
    }
    
    public RecordTypeNotFoundException(SchemaId id, Long version) {
        this.id = id != null ? id.toString() : null;
        this.name = null;
        this.version = version;
    }
    
    public RecordTypeNotFoundException(QName name, Long version) {
        this.id = null;
        this.name = name != null ? name.toString() : null;
        this.version = version;
    }
    
    @Override
    public String getMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("RecordType <");
        stringBuilder.append(id != null ? id : name);
        stringBuilder.append("> ");
        if (version != null) {
            stringBuilder.append("version: <");
            stringBuilder.append(version);
            stringBuilder.append("> ");
        }
        stringBuilder.append("could not be found.");
        return stringBuilder.toString();
    }
}
