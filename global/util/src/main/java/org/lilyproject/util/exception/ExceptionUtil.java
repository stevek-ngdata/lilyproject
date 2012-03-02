package org.lilyproject.util.exception;

public class ExceptionUtil {
    /**
     * Enable the thread interrupted flag if a throwable is an InterruptedException.
     */
    public static void handleInterrupt(Throwable t) {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
