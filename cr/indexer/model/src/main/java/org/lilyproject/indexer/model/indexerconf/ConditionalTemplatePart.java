/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.indexer.model.indexerconf;

public class ConditionalTemplatePart implements TemplatePart {
    private String conditional;
    private String trueString;
    private String falseString;

    public ConditionalTemplatePart(String conditional, String trueString, String falseString) {
        this.conditional = conditional;
        this.trueString = trueString;
        this.falseString = falseString;
    }

    public String getConditional() {
        return conditional;
    }

    public String getTrueString() {
        return trueString;
    }

    public String getFalseString() {
        return falseString;
    }



}
