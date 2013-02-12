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

import org.springframework.beans.factory.FactoryBean;

public class ObjectFactoryBean  implements FactoryBean {
    private final Class clazz;
    private final Object object;

    public ObjectFactoryBean(Class clazz, Object object) {
        this.clazz = clazz;
        this.object = object;
    }

    public Object getObject() throws Exception {
        return object;
    }

    public Class getObjectType() {
        return clazz;
    }

    public boolean isSingleton() {
        return true;
    }
}
