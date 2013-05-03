/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.api;

/**
 * Describes properties with which a repository table is to be created.
 */
public class TableCreateDescriptor {
    private String name;
    private byte[][] splitKeys;

    public TableCreateDescriptor(String name) {
        this.name = name;
    }

    public TableCreateDescriptor(String name, byte[][] splitKeys) {
        this.name = name;
        this.splitKeys = splitKeys;
    }

    /**
     * Returns the name of the repository table.
     */
    public String getName() {
        return name;
    }

    /**
     * Return the region split keys of the table.
     */
    public byte[][] getSplitKeys() {
        return splitKeys;
    }
}
