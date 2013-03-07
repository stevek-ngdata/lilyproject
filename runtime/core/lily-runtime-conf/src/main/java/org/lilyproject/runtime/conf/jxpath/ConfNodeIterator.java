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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.jxpath.ri.compiler.NodeTest;
import org.apache.commons.jxpath.ri.model.NodeIterator;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.lilyproject.runtime.conf.Conf;

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
            if (ConfNodePointer.testNode(child, nodeTest)) {
                children.add(child);
            }
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
