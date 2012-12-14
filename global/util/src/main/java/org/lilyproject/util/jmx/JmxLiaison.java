package org.lilyproject.util.jmx;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.Set;

/**
 * Abstracts between calling JMX methods on the current JVM or another one (of launch-test-lily or launch-hadoop).
 */
public class JmxLiaison {
    private MBeanServerConnection connection;
    private JMXConnector connector;

    public void connect(boolean embed) throws Exception {
        if (embed) {
            connection = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        } else {
            String hostport = "localhost:10102";
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + hostport + "/jndi/rmi://"
                    + hostport + "/jmxrmi");
            connector = JMXConnectorFactory.connect(url);
            connector.connect();
            connection = connector.getMBeanServerConnection();
        }
    }

    public void disconnect() throws Exception {
        if (connector != null)
            connector.close();
    }

    public Object getAttribute(ObjectName objectName, String attrName) throws Exception {
        return connection.getAttribute(objectName, attrName);
    }

    public Set<ObjectName> queryNames(ObjectName objectName) throws IOException {
        return connection.queryNames(objectName, null);
    }

    public Object invoke(ObjectName objectName, String operation, Object[] params, String[] signature) throws Exception {
        return connection.invoke(objectName, operation, params, signature);
    }

    public Object invoke(ObjectName objectName, String operation, String arg) throws Exception {
        return connection.invoke(objectName, operation, new Object[] { arg }, new String[] { "java.lang.String" });
    }
}
