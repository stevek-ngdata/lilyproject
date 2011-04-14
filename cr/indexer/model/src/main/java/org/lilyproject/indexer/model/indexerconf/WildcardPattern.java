package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.util.Pair;

/**
 * Simple wildcard matching: a star wildcard is supported at the start or the
 * end of the string.
 */
public class WildcardPattern {
    private final Type type;
    private final String string;

    enum Type { EQUALS, STARTS_WITH, ENDS_WITH }

    public WildcardPattern(String pattern) {
        // We require the pattern length to be at least 2, so that a star on itself
        // is not recognized as a wildcard.
        if (pattern.length() > 1 && pattern.startsWith("*")) {
            type = Type.ENDS_WITH;
            string = pattern.substring(1);
        } else if (pattern.length() > 1 && pattern.endsWith("*")) {
            type = Type.STARTS_WITH;
            string = pattern.substring(0, pattern.length() - 1);
        } else {
            type = Type.EQUALS;
            string = pattern;
        }
    }

    public Pair<Boolean, String> match(String input) {
        Pair<Boolean, String> result;
        switch (type) {
            case STARTS_WITH:
                if (input.startsWith(string)) {
                    result = new Pair<Boolean, String>(Boolean.TRUE, input.substring(string.length()));
                } else {
                    result = new Pair<Boolean, String>(Boolean.FALSE, null);
                }
                break;
            case ENDS_WITH:
                if (input.endsWith(string)) {
                    result = new Pair<Boolean, String>(Boolean.TRUE, input.substring(0, input.length() - string.length()));
                } else {
                    result = new Pair<Boolean, String>(Boolean.FALSE, null);
                }
                break;
            default:
                result = new Pair<Boolean, String>(input.equals(string), null);
                break;
        }
        return result;
    }

    /**
     * A match which doesn't bother to compute and return the text matched by the wildcard.
     */
    public boolean lightMatch(String input) {
        boolean result;
        switch (type) {
            case STARTS_WITH:
                result = input.startsWith(string);
                break;
            case ENDS_WITH:
                result = input.endsWith(string);
                break;
            default:
                result = input.equals(string);
                break;
        }
        return result;
    }

    /**
     * Returns true if the pattern actually contains a wildcard, thus when on successful match there will be
     * a value available that was matched by the wildcard.
     */
    public boolean hasWildcard() {
        return type == Type.STARTS_WITH || type == Type.ENDS_WITH;
    }

    public static boolean isWildcardExpression(String text) {
        return text.length() > 1 && (text.startsWith("*") || text.endsWith("*"));
    }
}
