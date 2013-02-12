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

import org.kauriproject.template.DefaultTemplateBuilder;
import org.kauriproject.template.SaxElement;
import org.kauriproject.template.TemplateBuilder;
import org.kauriproject.template.el.ELFacade;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;

/**
 * Test taglib implementation.
 */
public class TestTaglib implements Taglib {

    /**
     * The namespace of this taglib.
     */
    public static final String NAMESPACE = "http://kauriproject.org/template/taglib/test";

    public String getNamespace() {
        return NAMESPACE;
    }

    public void handleStart(TemplateBuilder builder, ELFacade elFacade, Locator locator, String localName,
            String name, Attributes attributes) {
        if (localName.equals("test")) {
            TestBlock block = new TestBlock(elFacade, locator, new SaxElement(NAMESPACE, localName, name,
                    attributes));
            ((DefaultTemplateBuilder) builder).pushBlock(block);
        }
    }

}
