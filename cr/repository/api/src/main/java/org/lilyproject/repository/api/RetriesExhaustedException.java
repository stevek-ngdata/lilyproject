package org.lilyproject.repository.api;

import java.util.HashMap;
import java.util.Map;

public class RetriesExhaustedException extends RepositoryException {
    private String operation;
    private int attempts;
    private long duration;

    /**
     * See {@link RepositoryException}
     */
    public RetriesExhaustedException(String message, Map<String, String> state) {
        // We construct our own message, ignore the message param
        this.operation = state.get("operation");
        String attempts = state.get("attempts");
        this.attempts = (attempts != null) ? Integer.valueOf(attempts) : null;
        String duration = state.get("duration");
        this.duration = (duration != null) ? Long.valueOf(duration) : null;
    }

    /**
     * See {@link RepositoryException}
     */
    @Override
    public Map<String, String> getState() {
        Map<String, String> state = new HashMap<String, String>();
        state.put("operation", operation);
        state.put("attempts", Integer.toString(attempts));
        state.put("duration", Long.toString(duration));
        return state;
    }

    public RetriesExhaustedException(String operation, int attempts, long duration, Throwable cause) {
        super(cause);
        this.operation = operation;
        this.attempts = attempts;
        this.duration = duration;
    }

    @Override
    public String getMessage() {
        return "Attempted " + operation + " operation " + attempts + " times during " + duration + " ms without success.";
    }
}
