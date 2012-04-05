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

import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A pattern matching system for {@link org.lilyproject.repository.api.ValueType} names.
 *
 * <p>ValueType itself does not define any structural requirements for the type argument,
 * i.e. it is up to each ValueType implementation to interpret the argument.</p>
 *
 * <p>The format for the patterns is described in the user documentation.</p>
 */
public class TypePattern {
    /**
     * Explanation of the pattern: it first matches a name which cannot contain &lt;>
     * or (), and then it has a parameter either surrounded by &lt;> or by (), or
     * actually they can be mixed like (> but that's not really intentional.
     */
    private static final Pattern TYPE_PATTERN = Pattern.compile("([^<>\\(\\)]+)(?:[<\\(](.+)[>\\)])?");
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<SubTypePattern> patterns = new ArrayList<SubTypePattern>();

    public TypePattern(String pattern) {
        this.patterns = parseOrPattern(pattern);
    }

    public boolean matches(String type) {
        ParsedType parsedType = parseType(type);

        for (SubTypePattern pattern : patterns) {
            if (pattern.matches(parsedType)) {
                return true;
            }
        }

        return false;
    }

    private ParsedType parseType(String type) {
        return parseType(type, type);
    }

    private ParsedType parseType(String type, String completeType) {

        Matcher matcher = TYPE_PATTERN.matcher(type);
        if (matcher.matches()) {
            String name = matcher.group(1);
            String arg = matcher.group(2);
            ParsedType typeArg = null;
            if (arg != null) {
                typeArg = parseType(arg, completeType);
            }
            return new ParsedType(name, typeArg);
        } else {
            throw new IllegalArgumentException("Invalid type: '" + type + "' in '" + completeType + "'");
        }
    }

    /**
     * Parse a comma-separated list of patterns.
     */
    private List<SubTypePattern> parseOrPattern(String pattern) {
        List<SubTypePattern> result = new ArrayList<SubTypePattern>();

        for (String typePattern : COMMA_SPLITTER.split(pattern)) {
            result.add(parsePattern(typePattern));
        }

        return result;
    }

    private SubTypePattern parsePattern(String pattern) {
        return parsePattern(pattern, pattern);
    }

    /**
     * Parse a type pattern, including its recursively contained type patterns for the arguments.
     */
    private SubTypePattern parsePattern(String pattern, String completePattern) {
        Matcher matcher = TYPE_PATTERN.matcher(pattern);
        if (matcher.matches()) {
            String name = matcher.group(1);
            TypeNameMatch typeNameMatch = new TypeNameMatch(name);

            String arg = matcher.group(2);
            TypeArgMatch typeArgMatch;
            if (arg == null) {
                typeArgMatch = new NoTypeArgumentMatch();
            } else if (arg.equals("*")) {
                typeArgMatch = new OneOptionalArgMatch();
            } else if (arg.equals("+")) {
                typeArgMatch = new ExactOneArgMatch();
            } else if (arg.equals("**")) {
                typeArgMatch = new AnyArgMatch();
            } else if (arg.equals("++")) {
                typeArgMatch = new AtLeastOneArgMatch();
            } else {
                typeArgMatch = new NestedArgMatch(parsePattern(arg, completePattern));
            }

            return new SubTypePattern(typeNameMatch, typeArgMatch);
        } else {
            throw new IllegalArgumentException("Invalid type pattern: '" + pattern + "' in '" + completePattern + "'");
        }
    }

    private static class ParsedType {
        String name;
        ParsedType arg;

        public ParsedType(String name, ParsedType arg) {
            this.name = name;
            this.arg = arg;
        }
    }

    private static class SubTypePattern {
        TypeNameMatch nameMatch;
        TypeArgMatch argMatch;

        public SubTypePattern(TypeNameMatch nameMatch, TypeArgMatch argMatch) {
            this.nameMatch = nameMatch;
            this.argMatch = argMatch;
        }

        public boolean matches(ParsedType type) {
            return nameMatch.matches(type.name) && argMatch.matches(type.arg);
        }
    }

    private static class TypeNameMatch {
        private WildcardPattern pattern;

        public TypeNameMatch(String name) {
            this.pattern = new WildcardPattern(name);
        }

        boolean matches(String name) {
            return pattern.lightMatch(name);
        }
    }

    private static interface TypeArgMatch {
        /**
         * @param type the argument type object to be checked, is null if there is no argument
         */
        boolean matches(ParsedType type);
    }

    private static class NoTypeArgumentMatch implements TypeArgMatch {
        @Override
        public boolean matches(ParsedType type) {
            return type == null;
        }
    }

    private static class ExactOneArgMatch implements TypeArgMatch {
        @Override
        public boolean matches(ParsedType type) {
            return type != null && type.arg == null;
        }
    }

    private static class OneOptionalArgMatch implements TypeArgMatch {
        @Override
        public boolean matches(ParsedType type) {
            return type == null || type.arg == null;
        }
    }

    private static class AtLeastOneArgMatch implements TypeArgMatch {
        @Override
        public boolean matches(ParsedType type) {
            return type != null;
        }
    }

    private static class AnyArgMatch implements TypeArgMatch {
        @Override
        public boolean matches(ParsedType type) {
            return true;
        }
    }

    private static class NestedArgMatch implements TypeArgMatch {
        private SubTypePattern pattern;

        public NestedArgMatch(SubTypePattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean matches(ParsedType type) {
            return type != null && pattern.matches(type);
        }
    }
}
