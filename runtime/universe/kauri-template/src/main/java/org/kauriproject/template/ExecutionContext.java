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
package org.kauriproject.template;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.kauriproject.template.el.FunctionRegistry;
import org.kauriproject.template.source.SourceResolver;
import org.kauriproject.template.security.AccessDecider;

/**
 * Representation of the execution context needed for generation of kauri templates.
 */
public class ExecutionContext {
    public static ThreadLocal<ExecutionContext> CURRENT = new ThreadLocal<ExecutionContext>();

    private SourceResolver sourceResolver;
    private AccessDecider accessDecider;

    private TemplateContext templateContext;

    private FunctionRegistry functionRegistry;
    private TemplateExecutor executor;
    private Map<String, Object> attributes = new HashMap<String, Object>();

    private Map<String, MacroBlock> macroRegistry;
    private Map<String, IncludeBlock> includeRegistry;
    private Map<String, List<InheritanceBlock>> inheritanceChain;
    private Stack<SuperBlock> superBlockStack;
    private Stack<List<InitBlock>> initStack;

    /** Name of the active i18n bundle, if any */
    private String i18nBundle;

    private int inherited = 0;
    private int silenceLevel = 0;
    private boolean extending = false;

    private String baseUri;

    public ExecutionContext() {
        this(null, null, null, null);
    }

    public ExecutionContext(SourceResolver sourceResolver, TemplateContext templateContext,
            FunctionRegistry functionRegistry, AccessDecider accessDecider) {
        this.sourceResolver = sourceResolver;
        this.templateContext = templateContext;
        this.functionRegistry = functionRegistry;
        this.accessDecider = accessDecider;
        this.macroRegistry = new HashMap<String, MacroBlock>();
        this.includeRegistry = new HashMap<String, IncludeBlock>();
        this.inheritanceChain = new HashMap<String, List<InheritanceBlock>>();
        this.superBlockStack = new Stack<SuperBlock>();
        this.initStack = new Stack<List<InitBlock>>();
        saveInit();
    }

    public SourceResolver getSourceResolver() {
        return sourceResolver;
    }

    public void setSourceResolver(SourceResolver sourceResolver) {
        this.sourceResolver = sourceResolver;
    }

    public AccessDecider getAccessDecider() {
        return accessDecider;
    }

    public void setAccessDecider(AccessDecider accessDecider) {
        this.accessDecider = accessDecider;
    }

    public TemplateContext getTemplateContext() {
        return templateContext;
    }

    public void setTemplateContext(TemplateContext templateContext) {
        this.templateContext = templateContext;
    }

    public FunctionRegistry getFunctionRegistry() {
        return functionRegistry;
    }

    public void setFunctionRegistry(FunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
    }

    public void inheritInc() {
        this.inherited++;
    }

    public void inheritDecr() {
        this.inherited--;
    }

    public boolean isInherited() {
        return inherited > 0;
    }

    public void setSilencing(boolean silencing) {
        if (silencing) {
            silenceLevel++;
        } else {
            silenceLevel--;
        }
    }

    public boolean isSilencing() {
        return silenceLevel > 0;
    }

    public Map<String, MacroBlock> getMacroRegistry() {
        return macroRegistry;
    }

    public Map<String, List<InheritanceBlock>> getInheritanceChain() {
        return inheritanceChain;
    }

    public Map<String, IncludeBlock> getIncludeRegistry() {
        return includeRegistry;
    }

    public Stack<SuperBlock> getSuperBlockStack() {
        return superBlockStack;
    }

    public TemplateExecutor getExecutor() {
        return executor;
    }

    public void setExecutor(TemplateExecutor executor) {
        this.executor = executor;
    }

    /**
     * A free set of attributes. This is intended for template users to make
     * non-predefined data available for during execution, e.g. for use
     * by custom expression language functions. This map should not be
     * modified during template execution. 
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }
    
    public List<InitBlock> getInitList() {
        return initStack.peek();
    }
    
    public List<InitBlock> saveInit() {
        List<InitBlock> list = new ArrayList<InitBlock>();
        return initStack.push(list);
    }
    
    public List<InitBlock> restoreInit() {
        return initStack.pop();
    }

    public void setExtending(boolean extending) {
        this.extending = extending;
    }
    
    public boolean isExtending() {
        return extending;
    }
    
    public String getBaseUri() {
        return baseUri;
    }

    public void setBaseUri(String baseUri) {
        this.baseUri = baseUri;
    }

    public String getI18nBundle() {
        return i18nBundle;
    }

    public void setI18nBundle(String i18nBundle) {
        this.i18nBundle = i18nBundle;
    }
}
