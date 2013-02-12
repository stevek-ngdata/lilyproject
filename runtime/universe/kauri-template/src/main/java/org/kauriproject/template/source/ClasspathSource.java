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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.kauriproject.util.io.IOUtils;

public class ClasspathSource implements Source {

    private URL url;
    private String reference;

    public ClasspathSource(URL url, String reference) {
        this.url = url;
        this.reference = reference;
    }

    public String getReference() {
        return reference;
    }

    public long getContentLength() {
        return -1;
    }

    public InputStream getInputStream() throws IOException {
        return url.openStream();
    }

    public void write(OutputStream outputStream) throws IOException {
        byte[] buff = new byte[8192];
        int read;
        InputStream is = null;
        try {
            is = getInputStream();
            while ((read = is.read(buff)) != -1) {
                outputStream.write(buff, 0, read);
            }
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    public void write(Writer writer) throws IOException {
        char[] buff = new char[8192];
        int read;
        InputStream is = null;
        try {
            is = getInputStream();
            Reader reader = new InputStreamReader(is, "UTF-8"); // ClasspathSource is not important enough right now to care much about encoding
            while ((read = reader.read(buff)) != -1) {
                writer.write(buff, 0, read);
            }
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    public URI getURI() {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Couldn't convert jar source URL to an URI: " + url.toString(), e);
        }
    }

    public Validity getValidity() {
        // Assumes jar files never change
        return new AlwaysValidValidity();
    }

    public String getMediaType() {
        String mediaType = URLConnection.getFileNameMap().getContentTypeFor(reference);

        if (mediaType == null) {
            if (reference.endsWith(".json")) {
                mediaType = "application/json";
            } else {
                mediaType = "application/octet-stream";
            }
        }
        
        return mediaType;
    }
    
    public String getEncoding() {
        return System.getProperty("file.encoding"); // just the default encoding. Note for HTML and XML a better effort could be done baed on the content.
    }

    public void release() {
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return new HashCodeBuilder(-883417899, 475076967).append(this.url).toHashCode();
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object object) {
        if (!(object instanceof ClasspathSource)) {
            return false;
        }
        ClasspathSource rhs = (ClasspathSource) object;
        return new EqualsBuilder().append(this.url, rhs.url).isEquals();
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return new ToStringBuilder(this).append("Reference", this.getReference()).toString();
    }

    public Object getSourceContext() {
        return null;
    }

    public String getReferenceInContext() {
        return getReference();
    }
}

