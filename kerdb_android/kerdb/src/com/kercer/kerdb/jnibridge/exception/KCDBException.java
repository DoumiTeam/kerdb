package com.kercer.kerdb.jnibridge.exception;

public class KCDBException extends Exception
{

    private static final long serialVersionUID = 1L;

    public KCDBException()
    {
        super();
    }

    public KCDBException(String error)
    {
        super(error);
    }

    public KCDBException(String error, Throwable cause)
    {
        super(error, cause);
    }
}
