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
import java.lang.reflect.Type;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.codehaus.jackson.map.ObjectMapper;
import org.lilyproject.indexer.model.api.IndexDefinition;

/**
 * MessageBodyWriter for writing IndexDefinition instances 
 * @author karel
 */
@Provider
public class IndexDefinitionMessageBodyWriter implements MessageBodyWriter<IndexDefinition> {

	public long getSize(IndexDefinition indexDefinition, Class<?> type,
			Type genericType, Annotation[] annotations, MediaType mediaType) {
		return -1;
	}

	public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
			MediaType mediaType) {
		if (mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
			return true;
		}
		return false;
	}

	public void writeTo(IndexDefinition indexDefinition, Class<?> type,
			Type genericType, Annotation[] annotations, MediaType mediaType,
			MultivaluedMap<String, Object> httpHeaders, OutputStream outputStream)
			throws IOException, WebApplicationException {
		//TODO: fine-tune output
		new ObjectMapper().writeValue(outputStream, indexDefinition);
	}
	

}
