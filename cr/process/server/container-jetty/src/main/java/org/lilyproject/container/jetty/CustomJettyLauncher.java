package org.lilyproject.container.jetty;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.container.api.Container;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class CustomJettyLauncher implements Container {

    private Log log = LogFactory.getLog(CustomJettyLauncher.class);

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
        return server;
    }

    public void addServletMapping(String urlPattern, HttpServlet servlet) {
        ServletHolder servletHolder = new ServletHolder(servlet);
        context.addServlet(servletHolder, urlPattern);
    }

}
