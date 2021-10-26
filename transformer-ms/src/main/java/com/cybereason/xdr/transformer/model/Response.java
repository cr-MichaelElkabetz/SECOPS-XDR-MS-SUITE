package com.cybereason.xdr.transformer.model;

public class Response {
    String message;

    public Response(String message) {
        this.setMessage(message);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
