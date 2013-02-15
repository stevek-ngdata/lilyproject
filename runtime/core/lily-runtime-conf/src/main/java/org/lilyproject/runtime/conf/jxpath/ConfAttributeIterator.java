package org.lilyproject.runtime.conf.jxpath;

import org.apache.commons.jxpath.ri.model.NodeIterator;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.ri.QName;
import org.lilyproject.runtime.conf.Conf;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ConfAttributeIterator implements NodeIterator {
    private NodePointer parent;
    private List<Map.Entry<String, String>> attributes = new ArrayList<Map.Entry<String, String>>();
    private int position = 0;

    /**
     * @param parent pointer
     * @param qName to test
     */
    public ConfAttributeIterator(NodePointer parent, QName qName) {
        this.parent = parent;

        Conf conf = (Conf)parent.getNode();

        String name = qName.getName();
        if (name.equals("*")) {
            attributes.addAll(conf.getAttributes().entrySet());
        } else {
            for (Map.Entry<String, String> entry : conf.getAttributes().entrySet()) {
                if (entry.getKey().equals(name)) {
                    attributes.add(entry);
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

        Map.Entry<String, String> entry = attributes.get(index);
        return new ConfAttributePointer(parent,  new ConfAttr(entry.getKey(), entry.getValue()));
    }

    public int getPosition() {
        return position;
    }

    public boolean setPosition(int position) {
        this.position = position;
        return position >= 1 && position <= attributes.size();
    }

}
