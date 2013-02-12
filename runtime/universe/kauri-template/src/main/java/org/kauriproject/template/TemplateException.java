package org.kauriproject.template;

public class TemplateException extends RuntimeException {
    public TemplateException() {
        super();
    }

    public TemplateException(String message) {
        super(message);
    }

    public TemplateException(String message, Throwable cause) {
        super(message, cause);
    }

    public TemplateException(Throwable cause) {
        super(cause);
    }
}
