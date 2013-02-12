package org.lilyproject.runtime.conf.jxpath;

import org.apache.commons.jxpath.ri.model.NodeIterator;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.ri.compiler.NodeTest;
import org.lilyproject.runtime.conf.Conf;

import java.util.List;
import java.util.ArrayList;

public class ConfNodeIterator implements NodeIterator {
    private NodePointer parent;
    private int position = 0;
    private List<Conf> children = new ArrayList<Conf>();

    /**
     * Create a new DOMNodeIterator.
     * @param parent parent pointer
     * @param nodeTest test
     * @param startWith starting pointer
     */
    public ConfNodeIterator(NodePointer parent, NodeTest nodeTest, NodePointer startWith) {
        this.parent = parent;

        Conf conf = (Conf) parent.getNode();
        for (Conf child : conf.getChildren()) {
            if (ConfNodePointer.testNode(child, nodeTest))
                children.add(child);
        }

        if (startWith != null) {
            Conf wanted = (Conf)startWith.getNode();
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i) == wanted) {
                    position = i + 1;
                    break;
                }
            }
        }
    }

    public NodePointer getNodePointer() {
        if (position == 0) {
            if (!setPosition(1)) {
                return null;
            }
            position = 0;
        }
        int index = position - 1;
        if (index < 0) {
            index = 0;
        }

        return new ConfNodePointer(parent,  children.get(index));
    }

    public int getPosition() {
        return position;
    }

    public boolean setPosition(int position) {
        this.position = position;
        return position >= 1 && position <= children.size();
    }
}
