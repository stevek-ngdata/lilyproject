/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.tools.import_.json;

public class ValueTypeNSConverter {
    public static String fromJson(String valueType, Namespaces namespaces) throws JsonFormatException {
        int dollarPos = valueType.indexOf('$');
        if (dollarPos == -1)
            return valueType;

        int ltPos = valueType.lastIndexOf('<');
        String prefix = valueType.substring(0, ltPos + 1);
        String nsPrefix = valueType.substring(ltPos + 1, dollarPos);
        String postfix = valueType.substring(dollarPos + 1);
        String uri = namespaces.getNamespace(nsPrefix);
        if (uri == null) {
            throw new JsonFormatException("Undefined prefix in qualified name: " + valueType);
        }
        return prefix+'{'+uri+'}'+postfix;
    }

    public static String toJson(String valueType, Namespaces namespaces) {
        int leftBracketPos = valueType.indexOf('{');
        if (leftBracketPos == -1)
            return valueType;

        int rightBracketPos = valueType.indexOf('}');
        String prefix = valueType.substring(0, leftBracketPos);
        String uri = valueType.substring(leftBracketPos + 1, rightBracketPos);
        String postfix = valueType.substring(rightBracketPos+1);
        return prefix + namespaces.getOrMakePrefix(uri)+'$'+postfix;
    }
}
