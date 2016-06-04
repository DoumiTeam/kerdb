package com.kercer.kerdb.jnibridge;

import android.util.Log;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class KCIterator extends KCNativeObject
{
    private static final String ASSERT_ITER_MSG = "Iterator reference is not existent";


    KCIterator(long aIterPtr)
    {
        super(aIterPtr);
    }

    @Override
    protected void releaseNativeObject(long ptr)
    {
        if (mPtr != 0)
        {
            nativeClose(ptr);
        }
        mPtr = 0;
    }

    @Override
    protected void finalize() throws Throwable
    {
        if (mPtr != 0)
        {
            Log.w("KeyIterator", "iterators must be closed");
            close();
        }
        super.finalize();
    }

    public void seekToFirst() throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativeSeekToFirst(mPtr);
    }

    public void seekToLast() throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativeSeekToLast(mPtr);
    }

    public void seek(byte[] target) throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        if (target == null)
        {
            throw new IllegalArgumentException();
        }
        nativeSeek(mPtr, target);
    }

    public boolean isValid() throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        return nativeValid(mPtr);
    }

    public void next() throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativeNext(mPtr);
    }

    public void prev() throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativePrev(mPtr);
    }

    public byte[] getKey() throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        return nativeKey(mPtr);
    }

    public byte[] getValue() throws KCDBException
    {
        assertNativePtr(ASSERT_ITER_MSG);
        return nativeValue(mPtr);
    }



    private static native void nativeClose(long ptr);
    private static native void nativeSeekToFirst(long ptr);
    private static native void nativeSeekToLast(long ptr);
    private static native void nativeSeek(long ptr, byte[] key);
    private static native boolean nativeValid(long ptr);
    private static native void nativeNext(long ptr);
    private static native void nativePrev(long ptr);
    private static native byte[] nativeKey(long ptr);
    private static native byte[] nativeValue(long ptr);

    //not use
    native String[] nativeIteratorNextArray(long ptr, String endPrefix, boolean reverse, int max) throws KCDBException;
    native boolean nativeIteratorIsValid(long ptr, String endPrefix, boolean reverse);
}
