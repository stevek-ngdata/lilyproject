package org.lilyproject.runtime.conf;

/**
 * An exception that can be thrown if there is something wrong in the configuration.
 *
 * <p>This is used by the {@link Conf} itself, e.g. in case of a missing required attribute
 * or child, but can also be thrown by your own code.
 */
public class ConfException extends RuntimeException {
    public ConfException() {
        super();
    }

    public ConfException(String message) {
        super(message);
    }

    public ConfException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfException(Throwable cause) {
        super(cause);
    }
}
