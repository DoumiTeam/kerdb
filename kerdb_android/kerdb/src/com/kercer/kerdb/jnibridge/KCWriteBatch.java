package com.kercer.kerdb.jnibridge;

import com.kercer.kerdb.KCDB;
import com.kercer.kerdb.jnibridge.exception.KCDBException;

import java.nio.ByteBuffer;

public class KCWriteBatch extends KCNativeObject
{
    private KCDB mDB;
    private static final String ASSERT_WRITEBATCH_MSG = "WriteBatch reference is not existent";

    public KCWriteBatch()
    {
        super(nativeCreate());
    }

    public KCWriteBatch(KCDB aDB)
    {
        this();
        mDB = aDB;
    }

    @Override
    protected void releaseNativeObject(long ptr)
    {
        nativeDestroy(ptr);
    }

    public void remove(ByteBuffer key) throws KCDBException
    {
        assertNativePtr(ASSERT_WRITEBATCH_MSG);
        if (key == null)
        {
            throw new NullPointerException("key");
        }

        nativeDelete(mPtr, key);
    }

    public void put(ByteBuffer key, ByteBuffer value) throws KCDBException
    {
        assertNativePtr(ASSERT_WRITEBATCH_MSG);
        if (key == null)
        {
            throw new NullPointerException("key");
        }
        if (value == null)
        {
            throw new NullPointerException("value");
        }

        nativePut(mPtr, key, value);
    }

    public void clear() throws KCDBException
    {
        assertNativePtr(ASSERT_WRITEBATCH_MSG);
        nativeClear(mPtr);
    }

    public void write() throws KCDBException
    {
        assertNativePtr(ASSERT_WRITEBATCH_MSG);
        if (mDB == null)
        {
            throw new IllegalStateException("mDB is null,when KCWriteBatch write");
        }
        mDB.write(this);
        clear();
    }



    private static native long nativeCreate();
    private static native void nativeDestroy(long ptr);
    private static native void nativeDelete(long ptr, ByteBuffer key);
    private static native void nativePut(long ptr, ByteBuffer key, ByteBuffer val);
    private static native void nativeClear(long ptr);
}
