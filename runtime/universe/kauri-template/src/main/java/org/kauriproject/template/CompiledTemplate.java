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

import java.util.List;
import java.util.Map;

import org.kauriproject.template.source.Validity;
import org.xml.sax.Locator;

/**
 * Representation of a compiled template by referencing its first "step".
 */
public class CompiledTemplate extends Step {
    private String location;
    private Validity validity;
    private Map<String, MacroBlock> macroRegistry;
    private Map<String, InheritanceBlock> inheritanceRegistry;
    private List<InitBlock> extendList;

    public CompiledTemplate(Locator locator) {
        super(locator);
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    // TODO: test validity

    public Validity getValidity() {
        return validity;
    }

    public void setValidity(Validity validity) {
        this.validity = validity;
    }

    public Map<String, MacroBlock> getMacroRegistry() {
        return macroRegistry;
    }

    public void setMacroRegistry(Map<String, MacroBlock> macroRegistry) {
        this.macroRegistry = macroRegistry;
    }

    public Map<String, InheritanceBlock> getInheritanceRegistry() {
        return inheritanceRegistry;
    }

    public void setInheritanceRegistry(Map<String, InheritanceBlock> inheritanceRegistry) {
        this.inheritanceRegistry = inheritanceRegistry;
    }

    public List<InitBlock> getExtendList() {
        return extendList;
    }

    public void setExtendList(List<InitBlock> extendList) {
        this.extendList = extendList;
    }

}
