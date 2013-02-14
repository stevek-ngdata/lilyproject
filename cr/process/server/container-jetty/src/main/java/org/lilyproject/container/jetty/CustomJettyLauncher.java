package org.lilyproject.container.jetty;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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

        server = new Server(12060);
        server.start();

        context = new Context(server, "/", Context.SESSIONS);
    }

    @PostConstruct
    public Server getServerInstance() throws Exception {
        for (ServletRegistryEntry entry: servletRegistry.getEntries()) {
            ServletHolder servletHolder = new ServletHolder(entry.getServletInstance(context.getServletContext()));
            for (String pattern: entry.getUrlPatterns()) {
                context.addServlet(servletHolder, pattern);
            }
        }

        return server;
    }

    public void addServletMapping(String urlPattern, HttpServlet servlet) {
        ServletHolder servletHolder = new ServletHolder(servlet);
    }

    @PreDestroy
    public void stopServer() throws Exception {
        server.stop();
    }

}
