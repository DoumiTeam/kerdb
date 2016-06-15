package com.kercer.kerdb.jnibridge;

import android.text.TextUtils;
import android.util.Log;

import com.kercer.kerdb.jnibridge.exception.KCDBException;
import com.kercer.kerdb.jnibridge.exception.KCIllegalStateException;

import java.io.Closeable;

abstract class KCNativeObject implements Closeable
{
    private static final String TAG = KCNativeObject.class.getSimpleName();
    protected long mPtr;
    private int mRefCount = 0;

    protected KCNativeObject()
    {
    }
    protected KCNativeObject(long aPtr)
    {
        this();
        if (aPtr != 0)
        {
            mPtr = aPtr;
            // The Java wrapper counts as one reference, will
            // be released when closed
            retain();
        }
    }

    synchronized public long getPtr()
    {
        return mPtr;
    }

    synchronized public void retain()
    {
        if (mPtr != 0)
            mRefCount++;
    }

    synchronized public void release()
    {
        if (mPtr != 0)
        {
            mRefCount--;
            if (mRefCount == 0)
            {
                releaseNativeObject(mPtr);
                mPtr = 0;
            }
        }
    }

    synchronized public void forceClose() throws KCDBException
    {
        if (mPtr != 0)
        {
            mRefCount = 0;
            releaseNativeObject(mPtr);
            mPtr = 0;
        }
    }

    @Override
    public synchronized void close()
    {
        release();
    }

    protected void assertNativePtr(String aMsg) throws KCIllegalStateException
    {
        if (getPtr() == 0)
        {
            throw new KCIllegalStateException(aMsg);
        }
    }

    protected abstract void releaseNativeObject(long ptr);

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

            Log.w(TAG, "KCNativeObject " + name + " refcount: " + mRefCount + " id: " + System.identityHashCode(this) + " was finalized before native resource was closed, did you forget to call close()?");
        }

        super.finalize();
    }
}
