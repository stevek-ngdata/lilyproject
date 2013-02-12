/*
 * Copyright 2009 Outerthought bvba and Schaubroeck nv
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

import javax.el.*;
import java.lang.reflect.Method;

public class BaseELContext extends ELContext {
    private ELResolver elResolver;
    private FunctionMapper functionMapper;
    private VariableMapper variableMapper;

    public BaseELContext(ELResolver elResolver, FunctionMapper functionMapper, VariableMapper variableMapper) {
        this.elResolver = elResolver != null ? elResolver : new CompositeELResolver();
        this.functionMapper = functionMapper != null ? functionMapper : new EmptyFunctionMapper();
        this.variableMapper = variableMapper != null ? variableMapper : new EmptyVariableMapper();
    }

    public ELResolver getELResolver() {
        return elResolver;
    }

    public FunctionMapper getFunctionMapper() {
        return functionMapper;
    }

    public VariableMapper getVariableMapper() {
        return variableMapper;
    }

    private static class EmptyFunctionMapper extends FunctionMapper {
        public Method resolveFunction(String s, String s1) {
            return null;
        }
    }

    private static class EmptyVariableMapper extends VariableMapper {
        public ValueExpression resolveVariable(String s) {
            return null;
        }

        public ValueExpression setVariable(String s, ValueExpression valueExpression) {
            return null;
        }
    }
}
