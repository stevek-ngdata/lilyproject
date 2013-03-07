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
package org.lilyproject.runtime.conf.jxpath;

import java.util.Locale;

import org.apache.commons.jxpath.ri.JXPathContextReferenceImpl;
import org.apache.commons.jxpath.ri.QName;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.ri.model.NodePointerFactory;
import org.lilyproject.runtime.conf.Conf;

public class ConfPointerFactory implements NodePointerFactory {
    private static boolean registered = false;

    public static synchronized void register() {
        if (!registered) {
            registered = true;
            JXPathContextReferenceImpl.addNodePointerFactory(new ConfPointerFactory());
        }
    }

    public int getOrder() {
        return 5;
    }

    public NodePointer createNodePointer(QName name, Object bean, Locale locale) {
        return bean instanceof Conf ? new ConfNodePointer((Conf) bean, locale) : null;
    }

    public NodePointer createNodePointer(NodePointer parent, QName name, Object bean) {
        return bean instanceof Conf ? new ConfNodePointer(parent, (Conf) bean) : null;
    }
}
