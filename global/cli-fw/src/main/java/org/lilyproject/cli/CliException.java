package org.lilyproject.cli;

/**
 * CliException's can be used to stop the CLI tool with a certain exit code and
 * printing a message.
 *
 * <p>It shouldn't be used in cases where stack traces and nested exceptions
 * are important. This exception is just for flow control.</p>
 */
public class CliException extends RuntimeException {
    private int exitCode;

    public CliException(String message, int exitCode) {
        super(message);
        this.exitCode = exitCode;
    }

    public CliException(String message) {
        this(message, 1);
    }

    public int getExitCode() {
        return exitCode;
    }
}
