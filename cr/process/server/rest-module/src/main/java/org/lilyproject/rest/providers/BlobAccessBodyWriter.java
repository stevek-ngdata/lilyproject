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
package org.lilyproject.rest.providers;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import org.apache.commons.io.IOUtils;
import org.lilyproject.repository.api.BlobAccess;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.util.io.Closer;

@Provider
public class BlobAccessBodyWriter implements MessageBodyWriter<BlobAccess> {
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return BlobAccess.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(BlobAccess blobAccess, Class<?> type, Type genericType, Annotation[] annotations,
                        MediaType mediaType) {
        return blobAccess.getBlob().getSize();
    }

    @Override
    public void writeTo(BlobAccess blobAccess, Class<?> type, Type genericType, Annotation[] annotations,
                        MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {
        InputStream is = null;
        try {
            is = blobAccess.getInputStream();
            IOUtils.copyLarge(blobAccess.getInputStream(), entityStream);
        } catch (BlobException e) {
            throw new IOException("Error reading blob.", e);
        } finally {
            Closer.close(is);
        }
    }
}
