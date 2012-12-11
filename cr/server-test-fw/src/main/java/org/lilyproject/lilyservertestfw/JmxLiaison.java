package org.lilyproject.lilyservertestfw;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.Set;

class JmxLiaison {
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
}
