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
package org.lilyproject.tools.import_.json;

public class WriteOptions {
    public static final WriteOptions INSTANCE;
    static {
        INSTANCE = new WriteOptions();
        INSTANCE.makeImmutable();
    }

    private boolean immutable = false;

    /** For records, should schema information about the fields in the records be included? */
    private boolean includeSchema = false;

    public boolean getIncludeSchema() {
        return includeSchema;
    }

    public void setIncludeSchema(boolean includeSchema) {
        if (immutable)
            throw new RuntimeException("This WriteOptions instance is immutable.");

        this.includeSchema = includeSchema;
    }

    public void makeImmutable() {
        immutable = true;
    }
}
