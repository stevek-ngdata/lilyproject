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
package org.kauriproject.template;

import org.kauriproject.xml.sax.XmlConsumer;
import org.kauriproject.xml.sax.XmlFilter;
import org.xml.sax.SAXException;

public interface TemplateResult extends XmlConsumer {

    void flush() throws SAXException;

    void addAttribute(String uri, String localName, String qName, String type, String value) throws SAXException;

    void addFilter(XmlFilter filter);

    void popFilter();
}
