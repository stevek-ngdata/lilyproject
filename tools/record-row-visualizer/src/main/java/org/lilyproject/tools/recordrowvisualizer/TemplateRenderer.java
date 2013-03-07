package org.lilyproject.tools.recordrowvisualizer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class TemplateRenderer {

    private Configuration conf;
    public TemplateRenderer() {
        Configuration conf = new Configuration();
        conf.setClassForTemplateLoading(TemplateRenderer.class, "/org/lilyproject/tools/recordrowvisualizer");
        this.conf = conf;
    }

    public void render(String templateName, Map<String, Object> variables, OutputStream os) throws IOException, TemplateException {
        Template template = conf.getTemplate(templateName);

        Writer out = new OutputStreamWriter(os, "UTF-8");
        template.process(variables, out);
        out.flush();
    }
}
