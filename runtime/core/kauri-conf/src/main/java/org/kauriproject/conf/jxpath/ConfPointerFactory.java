package org.kauriproject.conf.jxpath;

import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.ri.model.NodePointerFactory;
import org.apache.commons.jxpath.ri.QName;
import org.apache.commons.jxpath.ri.JXPathContextReferenceImpl;
import org.kauriproject.conf.Conf;

import java.util.Locale;

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
