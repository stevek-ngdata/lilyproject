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

public interface SourceResolver {
    /**
     * During template execution, this thread local variable is set to the value
     * returned by {@link org.kauriproject.template.source.Source#getSourceContext()},
     * corresponding to the source from which the template instructions are loaded
     * (thus with proper switching when going to instructions from included from other
     * sources). 
     */
    public static ThreadLocal<Object> ACTIVE_SOURCE_CONTEXT = new ThreadLocal<Object>();

    /**
     * Resolves a uri to a Source object.
     *
     * <p>Note that this method migh be expensive, i.e. it might involve contacting
     * remote servers, or call heavy internal processes, ...
     *
     * <p><b>The returned {@link Source} object should be released after use by
     * callings its {@link Source#release} method to avoid resource leakage!</b></p>
     */
    public Source resolve(String sourceLocation);

    public Source resolve(String sourceLocation, String baseLocation);

    /**
     *
     * @param baseLocation allowed to be null
     * @param mediaTypes a comma-separated list of prefered/accepted media types, to be used
     *                   for content negotation. A SourceResolver is free to ignore this
     *                   information, so there is no guarantee the returned data will
     *                   match the supported media types.
     */
    public Source resolve(String sourceLocation, String baseLocation, String mediaTypes);

    public String toAbsolutePath(String sourceLocation, String baseLocation);

}
