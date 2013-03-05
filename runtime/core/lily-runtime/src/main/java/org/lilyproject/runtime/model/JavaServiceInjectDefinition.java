/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.runtime.model;

import org.lilyproject.util.ArgumentValidator;

public abstract class JavaServiceInjectDefinition {
    private String sourceModuleId;
    private String sourceJavaServiceName;

    /**
     *
     * @param sourceJavaServiceName optional (= can be null), in case the source module only
     *                               exports one service of the needed type.
     */
    public JavaServiceInjectDefinition(String sourceModuleId, String sourceJavaServiceName) {
        ArgumentValidator.notNull(sourceModuleId, "sourceModuleId");

        this.sourceModuleId = sourceModuleId;
        this.sourceJavaServiceName = sourceJavaServiceName;
    }

    public String getSourceModuleId() {
        return sourceModuleId;
    }

    public String getSourceJavaServiceName() {
        return sourceJavaServiceName;
    }
}
