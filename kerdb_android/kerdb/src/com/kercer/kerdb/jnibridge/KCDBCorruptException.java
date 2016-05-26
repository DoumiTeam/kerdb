package com.kercer.kerdb.jnibridge;

public class KCDBCorruptException extends KCDBException
{
    private static final long serialVersionUID = -2110293580518875321L;

    public KCDBCorruptException() {
    }

    public KCDBCorruptException(String error) {
        super(error);
    }
}
