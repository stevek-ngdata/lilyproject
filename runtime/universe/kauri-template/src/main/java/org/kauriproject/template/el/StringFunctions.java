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

/**
 * Basic string functions that can be used in EL-expressions.
 */
public class StringFunctions {

    public static String string(Object obj) {
        return obj.toString();
    }

    public static String toLowerCase(String string) {
        return string.toLowerCase();
    }

    public static String toUpperCase(String string) {
        return string.toUpperCase();
    }

    public static String concat(String... strings) {
        StringBuilder builder = new StringBuilder();
        for (String string : strings)
            builder.append(string);
        return builder.toString();
    }

    public static String trim(String string) {
        return string.trim();
    }

    public static int length(String string) {
        return string.length();
    }   

    public static String substring(String string, int beginIndex, Integer endIndex) {
        if (endIndex != null) {
            return string.substring(beginIndex, endIndex);
        }
        return string.substring(beginIndex);
    }

    public static String left(String string, int length) {
        return string.substring(0, length);
    }

    public static String right(String string, int length) {
        return string.substring(string.length() - length, string.length());
    }

    public static boolean startsWith(String string, String substring) {
        return string.startsWith(substring);
    }

    public static boolean endsWith(String string, String substring) {
        return string.endsWith(substring);
    }

    public static boolean contains(String string, String substring) {
        return string.contains(substring);
    }
    
    public static String replace(String string, String regex, String replacement) {
        return string.replaceAll(regex, replacement);
    }
}
