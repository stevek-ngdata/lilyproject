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

import java.net.URL;

/**
 * Simple sourceresolver for files on the classpath.
 */
public class ClasspathSourceResolver implements SourceResolver {

    public Source resolve(final String sourceLocation) {
        final URL url = resolveToUrl(sourceLocation);
        return new ClasspathSource(url, sourceLocation);
    }

    public Source resolve(final String sourceLocation, final String baseLocation) {
        return resolve(toAbsolutePath(sourceLocation, baseLocation));
    }

    public Source resolve(String sourceLocation, String baseLocation, String mediaTypes) {
        return resolve(toAbsolutePath(sourceLocation, baseLocation));
    }

    public String toAbsolutePath(final String sourceLocation, final String baseLocation) {
        if (sourceLocation.startsWith("/"))
            return sourceLocation;

        if (baseLocation.endsWith("/"))
            return baseLocation + sourceLocation;
        //else 
        int end = baseLocation.lastIndexOf('/');
        if (end < 0) {
            return sourceLocation;
        }
        return baseLocation.substring(0, ++end) + sourceLocation;
    }

    private URL resolveToUrl(final String sourceLocation) {
        URL url = getClass().getResource(sourceLocation);
        if (url == null && !sourceLocation.startsWith("/")) {
            // fallback: try leading slash
            url = getClass().getResource("/" + sourceLocation);
        }
        if (url == null) {
            throw new SourceException("Source not found: " + sourceLocation);
        }
        return url;
    }
}
