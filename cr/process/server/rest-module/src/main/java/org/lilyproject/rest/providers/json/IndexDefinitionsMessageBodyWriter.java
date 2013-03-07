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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.impl.IndexDefinitionConverter;
import org.lilyproject.util.json.JsonFormat;

/**
 * MessageBodyWriter for writing Collection<IndexDefinition> instances
 */
@Provider
public class IndexDefinitionsMessageBodyWriter implements MessageBodyWriter<Collection<IndexDefinition>> {

	@Override
    public long getSize(Collection<IndexDefinition> indices, Class<?> type,
			Type genericType, Annotation[] annotations, MediaType mediaType) {
		return -1;
	}

	@Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
			MediaType mediaType) {
		if (Collection.class.isAssignableFrom(type) && mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
		    if (genericType instanceof ParameterizedType) {
		        ParameterizedType parameterizedType = (ParameterizedType)genericType;
		        if (Arrays.asList(parameterizedType.getActualTypeArguments()).contains(IndexDefinition.class)) {
		            return true;
		        }
		    }
		}
		return false;
	}

	@Override
    public void writeTo(Collection<IndexDefinition> indices, Class<?> type,
			Type genericType, Annotation[] annotations, MediaType mediaType,
			MultivaluedMap<String, Object> httpHeaders, OutputStream outputStream)
			throws IOException, WebApplicationException {
	    ArrayNode array = JsonNodeFactory.instance.arrayNode();
	    IndexDefinitionConverter converter = IndexDefinitionConverter.INSTANCE;

	    for (IndexDefinition index : indices) {
	        array.add(converter.toJson(index));
	    }

	    IOUtils.write(JsonFormat.serializeAsBytes(array), outputStream);
	}
}