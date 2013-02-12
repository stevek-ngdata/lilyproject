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

/**
 * Enumeration of supported template directives.
 */
public enum Directive {

    /**
     * for-each directive<br>
     * forEach begin="x" end="y" step="z" var="item" in="collection"
     */
    FOREACH("forEach"),
    /**
     * if directive<br>
     * if test="expression"
     */
    IF("if"),
    /**
     * choose directive<br>
     * Directive for choose construct, with nested 'when' and (optional) otherwise instructions.
     */
    CHOOSE("choose"),
    /**
     * when directive<br>
     * when test="expression"
     */
    WHEN("when"),
    /**
     * otherwise directive<br>
     * Option (in choose construct) to execute when none of the when clauses evaluates to true.
     */
    OTHERWISE("otherwise"),
    /**
     * attribute directive<br>
     * attribute name="foo" value="bar"
     */
    ATTRIBUTE("attribute"),
    /**
     * element directive<br>
     * element name="foo"
     */
    ELEMENT("element"),
    /**
     * insert directive<br>
     * insert src="xmlfile.xml" mode="xml"
     */
    INSERT("insert"),
    /**
     * directive to insert another template<br>
     * include src="template-location"
     */
    INCLUDE("include"),
    /**
     * directive to import another template<br>
     * import src="template-location"
     */
    IMPORT("import"),
    /**
     * directive to create a variable<br>
     * variable name="foo" [value="bar"]
     */
    VARIABLE("variable"),
    /**
     * directive to generate comments in output
     */
    COMMENT("comment"),
    /**
     * directive to create a macro<br>
     * macro name="foobar"
     */
    MACRO("macro"),
    /**
     * directive to define a parameter to be used in a macro<br>
     * parameter name="foo" [value="bar"]
     */
    PARAMETER("parameter"),
    /**
     * directive to call a macro<br>
     * callMacro name="foobar"
     */
    CALLMACRO("callMacro"),
    /**
     * directive to define a block for template inheritance<br>
     * block name="blockid"
     */
    BLOCK("block"),
    /**
     * directive (in block construct) to call the overriden block
     */
    SUPERBLOCK("superBlock"),
    /**
     * directive to define an init block to be used in template inheritance
     */
    INIT("init"),
    PROTECT("protect"),
    TEXT("text");

    private final String tagName;

    Directive(String tagName) {
        this.tagName = tagName;
    }

    public String getTagName() {
        return tagName;
    }

    /**
     * Returns null if name is not recognized.
     */
    public static Directive fromString(String name) {
        if (name.equals(FOREACH.tagName))
            return FOREACH;
        else if (name.equals(IF.tagName))
            return IF;
        else if (name.equals(CHOOSE.tagName))
            return CHOOSE;
        else if (name.equals(WHEN.tagName))
            return WHEN;
        else if (name.equals(OTHERWISE.tagName))
            return OTHERWISE;
        else if (name.equals(ATTRIBUTE.tagName))
            return ATTRIBUTE;
        else if (name.equals(ELEMENT.tagName))
            return ELEMENT;
        else if (name.equals(INSERT.tagName))
            return INSERT;
        else if (name.equals(INCLUDE.tagName))
            return INCLUDE;
        else if (name.equals(IMPORT.tagName))
            return IMPORT;
        else if (name.equals(VARIABLE.tagName))
            return VARIABLE;
        else if (name.equals(COMMENT.tagName))
            return COMMENT;
        else if (name.equals(MACRO.tagName))
            return MACRO;
        else if (name.equals(PARAMETER.tagName))
            return PARAMETER;
        else if (name.equals(CALLMACRO.tagName))
            return CALLMACRO;
        else if (name.equals(BLOCK.tagName))
            return BLOCK;
        else if (name.equals(SUPERBLOCK.tagName))
            return SUPERBLOCK;
        else if (name.equals(INIT.tagName))
            return INIT;
        else if (name.equals(PROTECT.tagName))
            return PROTECT;
        else if (name.equals(TEXT.tagName))
            return TEXT;
        else
            return null;
    }
}
