package org.kauriproject.conf.jxpath;

import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.ri.*;
import org.apache.commons.jxpath.ri.compiler.NodeTest;
import org.apache.commons.jxpath.ri.compiler.NodeTypeTest;

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
