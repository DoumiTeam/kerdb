package com.kercer.kerdb.jnibridge;

/**
 * Created by zihong on 16/6/4.
 */
public class KCIllegalStateException extends KCDBException
{
    public KCIllegalStateException() {
    }

    public KCIllegalStateException(String error) {
        super(error);
    }
}
