package com.kercer.kerdb.jnibridge;

import android.util.Log;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class KCIterator extends KCNativeObject
{

    private  String mEndPrefix;
    private  boolean mReverse;
    private boolean mIsNextValid;
    private static final String ASSERT_ITER_MSG = "Iterator reference is not existent";


    KCIterator(long aIterPtr)
    {
        super(aIterPtr);
    }

    protected KCIterator(long aIterPtr, String aEndPrefix, boolean aReverse)
    {
        super(aIterPtr);
        this.mEndPrefix = aEndPrefix;
        this.mReverse = aReverse;

        mIsNextValid = nativeIteratorIsValid(aIterPtr, aEndPrefix, aReverse);
    }

    @Override
    protected void closeNativeObject(long ptr)
    {
        if (mPtr != 0)
        {
            nativeClose(ptr);
        }
        mPtr = 0;
        mIsNextValid = false;

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

    public void seekToFirst()
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativeSeekToFirst(mPtr);
    }

    public void seekToLast()
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativeSeekToLast(mPtr);
    }

    public void seek(byte[] target)
    {
        assertNativePtr(ASSERT_ITER_MSG);
        if (target == null)
        {
            throw new IllegalArgumentException();
        }
        nativeSeek(mPtr, target);
    }

    public boolean isValid()
    {
        assertNativePtr(ASSERT_ITER_MSG);
        return nativeValid(mPtr);
    }

    public void next()
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativeNext(mPtr);
    }

    public void prev()
    {
        assertNativePtr(ASSERT_ITER_MSG);
        nativePrev(mPtr);
    }

    public byte[] getKey()
    {
        assertNativePtr(ASSERT_ITER_MSG);
        return nativeKey(mPtr);
    }

    public byte[] getValue()
    {
        assertNativePtr(ASSERT_ITER_MSG);
        return nativeValue(mPtr);
    }

    public boolean hasNext()
    {
        return mIsNextValid;
    }

    public String[] next(int max)
    {
        if (!mIsNextValid)
        {
            throw new NoSuchElementException();
        }
        try
        {
            String[] keys = nativeIteratorNextArray(mPtr, mEndPrefix, mReverse, max);
            mIsNextValid = nativeIteratorIsValid(mPtr, mEndPrefix, mReverse);
            if (!mIsNextValid)
            {
                close();
            }
            return keys;
        }
        catch (KCDBException e)
        {
            throw new RuntimeException(e);
        }
    }

    private class KCBatchIterable implements Iterable<String[]>, Iterator<String[]>
    {

        private int size;

        private KCBatchIterable(int size)
        {
            this.size = size;
        }

        @Override
        public Iterator<String[]> iterator()
        {
            return this;
        }

        @Override
        public boolean hasNext()
        {
            return KCIterator.this.hasNext();
        }

        @Override
        public String[] next()
        {
            return KCIterator.this.next(size);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    public Iterable<String[]> byBatch(int size)
    {
        return new KCBatchIterable(size);
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

    native String[] nativeIteratorNextArray(long ptr, String endPrefix, boolean reverse, int max) throws KCDBException;
    native boolean nativeIteratorIsValid(long ptr, String endPrefix, boolean reverse);
}
