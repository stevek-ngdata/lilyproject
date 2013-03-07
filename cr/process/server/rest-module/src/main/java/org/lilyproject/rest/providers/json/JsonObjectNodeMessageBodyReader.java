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
package org.lilyproject.rest.providers.json;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

@Provider
public class JsonObjectNodeMessageBodyReader implements MessageBodyReader<ObjectNode> {
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
            if (type.isAssignableFrom(ObjectNode.class)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ObjectNode readFrom(Class<ObjectNode> clazz, Type type,
            Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> params, InputStream inputStream)
            throws IOException, WebApplicationException {
        ObjectMapper m = new ObjectMapper();
        return (ObjectNode)m.readTree(inputStream);
    }

}
