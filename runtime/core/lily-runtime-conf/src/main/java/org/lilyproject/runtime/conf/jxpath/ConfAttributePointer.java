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

import org.apache.commons.jxpath.ri.QName;
import org.apache.commons.jxpath.ri.compiler.NodeTest;
import org.apache.commons.jxpath.ri.compiler.NodeTypeTest;
import org.apache.commons.jxpath.ri.model.NodePointer;

public class ConfAttributePointer extends NodePointer {
    private ConfAttr attr;

    public ConfAttributePointer(NodePointer parent, ConfAttr attr) {
        super(parent);
        this.attr = attr;
    }

    public QName getName() {
        return new QName(null, attr.getName());
    }

    public Object getValue() {
        return attr.getValue();
    }

    public Object getBaseValue() {
        return attr;
    }

    public boolean isCollection() {
        return false;
    }

    public int getLength() {
        return 1;
    }

    public Object getImmediateNode() {
        return attr;
    }

    public boolean isActual() {
        return true;
    }

    public boolean isLeaf() {
        return true;
    }

    public boolean testNode(NodeTest nodeTest) {
        return nodeTest == null
            || ((nodeTest instanceof NodeTypeTest)
                && ((NodeTypeTest) nodeTest).getNodeType() == org.apache.commons.jxpath.ri.Compiler.NODE_TYPE_NODE);
    }

    public int hashCode() {
        return attr.hashCode();
    }

    public boolean equals(Object object) {
        return object == this || object instanceof ConfAttributePointer
                && attr.equals(((ConfAttributePointer) object).attr);
    }

    public int compareChildNodePointers(NodePointer pointer1,
            NodePointer pointer2) {
        // Won't happen - attributes don't have children
        return 0;
    }

    public void setValue(Object o) {
        throw new UnsupportedOperationException();
    }
}
