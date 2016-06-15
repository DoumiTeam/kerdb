package com.kercer.kerdb.jnibridge.exception;

/**
 * Created by zihong on 16/6/15.
 */
public class KCIOException extends KCDBException
{
    public KCIOException()
    {
    }

    public KCIOException(String error)
    {
        super(error);
    }
}
