/*
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.kauriproject.template.source;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;

/**
 * Blueprint of a source object.
 */
public interface Source {

    String getReference();

    URI getURI();

    InputStream getInputStream() throws IOException;

    /**
     * @param outputStream should be flushed and closed by the caller.
     */
    void write(OutputStream outputStream) throws IOException;

    /**
     * @param writer should be flushed and closed by the caller
     */
    void write(Writer writer) throws IOException;

    long getContentLength();

    String getMediaType();
    
    String getEncoding();

    Validity getValidity();

    /**
     * Frees any resources held by the source object.
     */
    void release();

    /**
     * Optional, implementation-defined object that will be made available
     * as 'current source context' during evaluation of template code loaded
     * from this source. Can be useful for SourceResolver or custom functions
     * to do things depending on where the template was loaded from.
     *
     * <p>The toString() method of the returned object will be used in the
     * cache key for caching templates.
     *
     * <p>This method may return null.
     */
    Object getSourceContext();

    /**
     * Similar to {@link #getReference}, but should return a reference that
     * is valid when the source context returned by {@link #getSourceContext}
     * is active.
     *
     * <p>In case there is no need for this, this method should return the
     * same as {@link #getReference}, this method should not return null.
     */
    String getReferenceInContext();
}
