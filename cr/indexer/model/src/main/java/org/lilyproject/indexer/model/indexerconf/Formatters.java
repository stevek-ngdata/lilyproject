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
package org.lilyproject.indexer.model.indexerconf;

import java.util.*;

public class Formatters {
    private Map<String, Formatter> formattersByName = new HashMap<String, Formatter>();
    private Formatter defaultFormatter = new DefaultFormatter();

    public Formatter getFormatter(String name) {
        if (name == null) {
            return getDefaultFormatter();
        }

        // During index configuration parsing, we will validate that all named formatters exist,
        // so the following check should never be true.
        if (!formattersByName.containsKey(name)) {
            throw new RuntimeException("Formatter does not exist: '" + name + "'");
        }

        return formattersByName.get(name);
    }

    public Formatter getDefaultFormatter() {
        return defaultFormatter;
    }

    protected boolean hasFormatter(String name) {
        return formattersByName.containsKey(name);
    }

    protected void addFormatter(Formatter formatter, String name) {
        formattersByName.put(name, formatter);
    }

    protected void setDefaultFormatter(String name) {
        defaultFormatter = formattersByName.get(name);
        if (defaultFormatter == null) {
            throw new RuntimeException("Default formatter does not exist: '" + name + "'");
        }
    }
}
