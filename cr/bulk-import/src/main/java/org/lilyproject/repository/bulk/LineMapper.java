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
package org.lilyproject.repository.bulk;


import org.lilyproject.repository.api.Record;

/**
 * Maps input lines into Lily {@link Record} objects for bulk import.
 */
public interface LineMapper {

    /**
     * Map a single input line into 0, 1, or many {@code Record} objects. Records can be created and via the supplied
     * {@link LineMappingContext}.
     * 
     * @param inputLine line to be mapped to a {@code Record}
     * @param context context for creating records and writing
     */
    void mapLine(String inputLine, LineMappingContext context);

}
