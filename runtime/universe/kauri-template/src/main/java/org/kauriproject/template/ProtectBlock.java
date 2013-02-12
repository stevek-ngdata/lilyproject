package org.kauriproject.template;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.kauriproject.template.security.AccessDecider;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;

public class ProtectBlock extends TemplateBlock {
    public static final String ACCESS_ATTR = "access";
    
    protected static Log log = LogFactory.getLog(ProtectBlock.class);

    protected ELFacade elFacade;

    public ProtectBlock(ELFacade elFacade, SaxElement saxElement) {
        super(saxElement);
        this.elFacade = elFacade;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        private final Expression accessExpr;

        public StartStep(Locator locator) {
            super(locator);

            Attributes attributes = getSaxElement().getAttributes();

            String accessAttr = attributes.getValue(ACCESS_ATTR);
            if (accessAttr == null)
                throw new TemplateException("protect instruction is missing required access attribute. Location: "
                        + getLocation());

            accessExpr = elFacade.createExpression(accessAttr, String.class);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) {
            AccessDecider decider = context.getAccessDecider();
            if (decider == null) {
                log.warn("protect instruction is used but no AccessDecider is provided.");
                return endStep.getCompiledNext();
            }
                
            String accessString = (String) accessExpr.evaluate(context.getTemplateContext());
            List<String> accessAttrs = new ArrayList<String>();

            if (accessString != null) {
                String[] accessAsArray = accessString.split(",");
                for (String accessAttr : accessAsArray) {
                    accessAttr = accessAttr.trim();
                    if (accessAttr.length() > 0)
                        accessAttrs.add(accessAttr);
                }
            }

            if (accessAttrs.size() == 0 || decider.decide(accessAttrs)) {
                return getCompiledNext();
            } else {
                return endStep.getCompiledNext();
            }

        }
    }

    class EndStep extends Step {

        public EndStep(Locator locator) {
            super(locator);
        }

    }
}
