package org.kauriproject.template.handling;

import java.io.IOException;
import java.io.Reader;

import org.codehaus.jackson.map.ObjectMapper;

public class JSONHandlingStrategy extends TextHandlingStrategy{

    @Override
    public Object parseToObject(HandlingInput input) throws IOException {
        ObjectMapper mapper = new ObjectMapper(); 
        Reader reader = input.getReader();
        Object obj = mapper.readValue(reader, Object.class);
        return obj;
    }
}
