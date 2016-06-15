package com.kercer.kerdb.jnibridge.exception;

/**
 * Created by zihong on 16/6/4.
 */
public class KCNullPointerException extends KCDBException
{
    public KCNullPointerException()
    {
    }

    public KCNullPointerException(String error)
    {
        super(error);
    }
}
