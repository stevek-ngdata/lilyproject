package org.lilyproject.container.jetty;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.container.api.Container;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;

public class CustomJettyLauncher implements Container {

    private Log log = LogFactory.getLog(CustomJettyLauncher.class);

    @Autowired
    private ServletRegistry servletRegistry;

    private Server server;
    private Context context;

    public CustomJettyLauncher() throws Exception {
        log.trace("in constructor custom jetty launcher");

        server = new Server(8899);

        context = new Context(server, "/", Context.SESSIONS);
        server.start();

    }

    @PostConstruct
    public Server getServerInstance() throws Exception {
        for (ServletRegistryEntry entry: servletRegistry.getEntries()) {
            context.addServlet(new ServletHolder(entry.getServletInstance(context.getServletContext())), entry.getUrlMapping());
        }
        return server;
    }

    public void addServletMapping(String urlPattern, HttpServlet servlet) {
        ServletHolder servletHolder = new ServletHolder(servlet);
    }

}
