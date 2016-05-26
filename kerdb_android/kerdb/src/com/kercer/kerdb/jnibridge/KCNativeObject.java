package com.kercer.kerdb.jnibridge;

import android.text.TextUtils;
import android.util.Log;

import java.io.Closeable;

abstract class KCNativeObject implements Closeable
{
    private static final String TAG = KCNativeObject.class.getSimpleName();
    protected long mPtr;
    private int mRefCount = 0;


    protected KCNativeObject()
    {
        // The Java wrapper counts as one reference, will
        // be released when closed
        ref();
    }

    protected KCNativeObject(long ptr)
    {
        this();

        if (ptr == 0)
        {
            throw new OutOfMemoryError("Failed to allocate native object");
        }

        mPtr = ptr;
    }

    synchronized protected long getPtr()
    {
        return mPtr;
    }

    protected void assertNativePtr(String aMsg)
    {
        if (getPtr() == 0)
        {
            throw new IllegalStateException(aMsg);
        }
    }

    synchronized void ref()
    {
        mRefCount++;
    }

    synchronized void unref()
    {
        if (mRefCount <= 0)
        {
            throw new IllegalStateException("Reference count is already 0");
        }

        mRefCount--;

        if (mRefCount == 0)
        {
            closeNativeObject(mPtr);
            mPtr = 0;
        }
    }

    protected abstract void closeNativeObject(long ptr);

    @Override
    public synchronized void close()
    {
        if (mPtr != 0)
        {
            unref();
        }
    }

    @Override
    protected void finalize() throws Throwable
    {
        if (mPtr != 0)
        {
            Class<?> clazz = getClass();
            String name = clazz.getSimpleName();
            while (TextUtils.isEmpty(name))
            {
                clazz = clazz.getSuperclass();
                name = clazz.getSimpleName();
            }

            Log.w(TAG, "NativeObject " + name + " refcount: " + mRefCount + " id: " + System.identityHashCode(this) + " was finalized before native resource was closed, did you forget to call close()?");
        }

        super.finalize();
    }
}
