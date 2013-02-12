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
package org.lilyproject.runtime.runtime.module.build;

/**
 * The type of a restservice is identified by a free-form string. This class
 * defined constants for special built-in types and methods for working with types.
 */
public class RestserviceType {
    public static final String NO_TYPE = "##none";
    public static final String ANY_TYPE = "##any";

    public static boolean isCompatible(String providedType, String expectedType) {
        if (providedType.equals(expectedType)) {
            return true;
        } else if (expectedType.equals(ANY_TYPE)) {
            return true;
        } else {
            return false;
        }
    }
}
