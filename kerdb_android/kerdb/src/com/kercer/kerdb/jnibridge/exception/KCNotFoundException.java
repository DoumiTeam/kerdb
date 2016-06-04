package com.kercer.kerdb.jnibridge.exception;

public class KCNotFoundException extends KCDBException
{
    private static final long serialVersionUID = 6207999645579440001L;

    public KCNotFoundException() {
    }

    public KCNotFoundException(String error) {
        super(error);
    }
}
