package org.lilyproject.repository.api;

import java.util.HashMap;
import java.util.Map;

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
        return "Unknown encoding '"+ encodingVersion+ "' encountered for a field of value type '" + valueType + "'";
    }
    
    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("valueType", valueType);
        state.put("encoding", encodingVersion);
        return state;
    }
}
