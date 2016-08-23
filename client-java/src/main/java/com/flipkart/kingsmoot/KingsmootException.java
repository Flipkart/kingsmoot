package com.flipkart.kingsmoot;

public class KingsmootException extends RuntimeException {
    public KingsmootException(String message) {
        super(message);
    }

    public KingsmootException(String message, Throwable cause) {
        super(message, cause);
    }
}
