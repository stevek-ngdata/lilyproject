package org.lilyproject.container.jetty.test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

/**
 * Basic Filter as target for integration testing.
 */
public class TestFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpreq = (HttpServletRequest) request;
        HttpServletResponse httpresp = (HttpServletResponse) response;
        String header = httpreq.getHeader("X-NGDATA-TEST");
        if (! StringUtils.isEmpty(header))
            httpresp.setHeader("X-NGDATA-TEST", header);
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
