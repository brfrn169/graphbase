package com.github.brfrn169.graphbase.exception;

public class GraphbaseException extends RuntimeException {

    private static final long serialVersionUID = -7047578455419128496L;

    public GraphbaseException() {
    }

    public GraphbaseException(String message) {
        super(message);
    }

    public GraphbaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public GraphbaseException(Throwable cause) {
        super(cause);
    }
}
