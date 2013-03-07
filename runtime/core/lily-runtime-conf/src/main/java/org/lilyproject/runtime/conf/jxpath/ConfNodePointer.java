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

import java.util.List;
import java.util.Locale;

import org.apache.commons.jxpath.ri.QName;
import org.apache.commons.jxpath.ri.compiler.NodeNameTest;
import org.apache.commons.jxpath.ri.compiler.NodeTest;
import org.apache.commons.jxpath.ri.compiler.NodeTypeTest;
import org.apache.commons.jxpath.ri.model.NodeIterator;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.lilyproject.runtime.conf.Conf;

public class ConfNodePointer extends NodePointer {
    private Conf conf;

    public ConfNodePointer(Conf conf, Locale locale) {
        super(null, locale);
        this.conf = conf;
    }

    public ConfNodePointer(NodePointer parent, Conf conf) {
        super(parent);
        this.conf = conf;
    }

    public boolean isLeaf() {
        return !conf.hasChildren();
    }

    public boolean isCollection() {
        return false;
    }

    public int getLength() {
        return 1;
    }

    public QName getName() {
        return new QName(null, conf.getName());
    }

    public Object getBaseValue() {
        return conf;
    }

    public Object getImmediateNode() {
        return conf;
    }

    public void setValue(Object o) {
        throw new UnsupportedOperationException();
    }

    public NodeIterator childIterator(NodeTest test, boolean reverse, NodePointer startWith) {
        return new ConfNodeIterator(this, test, startWith);
    }

    public NodeIterator attributeIterator(QName name) {
        return new ConfAttributeIterator(this, name);
    }

    public boolean isActual() {
        return true;
    }

    public int compareChildNodePointers(NodePointer pointer1, NodePointer pointer2) {
        Object value1 = pointer1.getBaseValue();
        Object value2 = pointer2.getBaseValue();
        if (value1 == value2) {
            return 0;
        }

        boolean value1isAttr = value1 instanceof ConfAttr;
        boolean value2isAttr = value2 instanceof ConfAttr;

        if (value1isAttr && !value2isAttr) {
            return -1;
        }
        if (!value1isAttr && value2isAttr) {
            return 1;
        }

        if (value1isAttr && value2isAttr) {
            return ((ConfAttr)value1).getName().compareTo(((ConfAttr)value2).getName());
        }

        List<Conf> children = conf.getChildren();
        for (Conf child : children) {
            if (child == value1) {
                return -1;
            }
            if (child == value2) {
                return 1;
            }
        }

        return 0;
    }

    public int hashCode() {
        return conf.hashCode();
    }

    public boolean equals(Object object) {
        return object == this || object instanceof ConfNodePointer && conf == ((ConfNodePointer)object).conf;
    }

    public Object getValue() {
        return conf.getValue(null);
    }

    public boolean testNode(NodeTest test) {
        return testNode(conf, test);
    }

    /**
     * Test a Node.
     * @param conf to test
     * @param test to execute
     * @return true if node passes test
     */
    public static boolean testNode(Conf conf, NodeTest test) {
        if (test == null) {
            return true;
        }

        if (test instanceof NodeNameTest) {
            NodeNameTest nodeNameTest = (NodeNameTest) test;
            return nodeNameTest.isWildcard() || nodeNameTest.getNodeName().getName().equals(conf.getName());
        }

        if (test instanceof NodeTypeTest) {
            return ((NodeTypeTest) test).getNodeType() == org.apache.commons.jxpath.ri.Compiler.NODE_TYPE_NODE;
        }

        return false;
    }
}
