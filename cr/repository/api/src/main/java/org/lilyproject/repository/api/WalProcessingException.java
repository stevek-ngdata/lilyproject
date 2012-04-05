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


public class WalProcessingException extends RepositoryException {
    private String recordId;
    private String info;

    public WalProcessingException(String message, Map<String, String> state) {
        this.recordId = state.get("recordId");
        this.info = state.get("info");
    }
    
    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("recordId", recordId);
        state.put("info", info);
        return state;
    }
    
    public WalProcessingException(RecordId recordId, String info) {
        this.info = info;
        this.recordId = recordId != null ? recordId.toString() : null;
    }

    public WalProcessingException(RecordId recordId, Throwable cause) {
        super(cause);
        this.recordId = recordId != null ? recordId.toString() : null;
    }

    @Override
    public String getMessage() {
        return "Wal failed to process messages for record '" + recordId + "', " + info;
    }
}
