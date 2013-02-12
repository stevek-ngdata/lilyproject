/*
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
package org.kauriproject.template.el;

import java.util.Iterator;

import javax.el.*;

import org.kauriproject.template.TemplateContext;


/**
 * The EL context to use during expression evaluation. During expression parsing, use
 * {@link ParseContext}.
 */
public class EvaluationContext extends BaseELContext {

    public EvaluationContext(TemplateContext templateContext) {
        super(getResolver(templateContext), null, null);
    }

    private static ELResolver getResolver(TemplateContext templateContext) {
        CompositeELResolver resolver = new CompositeELResolver();
        resolver.add(new TemplateELResolver(templateContext));
        resolver.add(new ArrayELResolver(true));
        resolver.add(new ListELResolver(true));
        resolver.add(new MapELResolver(true));
        resolver.add(new ResourceBundleELResolver());
        resolver.add(new BeanELResolver(true));
        return resolver;
    }

    private static class TemplateELResolver extends ELResolver {
        TemplateContext templateContext;

        public TemplateELResolver(TemplateContext templateContext) {
            this.templateContext = templateContext;
        }

        public Object getValue(ELContext context, Object base, Object property) {
            if (base == null) {
                context.setPropertyResolved(true);
                return templateContext.get((String)property);
            }
            return null;
        }

        public Class<?> getType(ELContext context, Object base, Object property) {
            if (base == null) {
                context.setPropertyResolved(true);
                return java.lang.Object.class; // doesn't matter, we're not writeable anyway
            }
            return null;
        }

        public void setValue(ELContext context, Object base, Object property, Object value) {
            if (base == null) {
                throw new PropertyNotWritableException("read only");
            }
        }

        public boolean isReadOnly(ELContext context, Object base, Object property) {
            if (base == null) {
                context.setPropertyResolved(true);
                return true;
            }
            return false;
        }

        public Iterator<java.beans.FeatureDescriptor> getFeatureDescriptors(ELContext context, Object base) {
            return null;
        }

        public Class<?> getCommonPropertyType(ELContext context, Object base) {
            if (base == null) {
                return java.lang.Object.class;
            }
            return null;
        }
    }

}
