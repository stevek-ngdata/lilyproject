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
package org.kauriproject.template.taglib;

import org.kauriproject.template.TemplateBuilder;
import org.kauriproject.template.el.ELFacade;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;

/**
 * Common interface that all custom taglibs must implement.
 */
public interface Taglib {

    /**
     * Each taglib has it's dedicated namespace.
     */
    public String getNamespace();

    public void handleStart(TemplateBuilder builder, ELFacade elFacade, Locator locator, String localName,
            String name, Attributes attributes);

}
