package com.kercer.kerdb.jnibridge;

import android.text.TextUtils;

import com.kercer.kerdb.KCDB;
import com.kercer.kerdb.KCDBObject;
import com.kercer.kerdb.KCDBOptions;
import com.kercer.kerdb.jnibridge.exception.KCDBException;
import com.kercer.kerdb.jnibridge.exception.KCNullPointerException;

import java.io.File;
import java.nio.ByteBuffer;

public class KCDBNative extends KCNativeObject implements KCDB
{
    private static final String LIB_NAME = "kerdb";
    private static final int LIMIT_MAX = Integer.MAX_VALUE - 8;
    private static final String ASSERT_DB_MSG = "Database reference is not existent (it has probably been closed)";

    private final File mPath;
    private boolean mDestroyOnClose = false;

    KCDBOptions mDBOptions;

    public KCDBNative(File aPath)
    {
        this(aPath, null);
    }

    public KCDBNative(File aPath, KCDBOptions aOptions)
    {
        super();

        if (aPath == null)
        {
            throw new NullPointerException();
        }
        mPath = aPath;

        if (aOptions == null)
        {
            mDBOptions = new KCDBOptions();
        }
    }


    @Override
    public void open() throws KCDBException
    {
        mPtr = nativeOpen(mPath.getAbsolutePath(),
                mDBOptions.createIfMissing(),
                mDBOptions.cacheSize(),
                mDBOptions.blockSize(),
                mDBOptions.writeBufferSize(),
                mDBOptions.errorIfExists(),
                mDBOptions.paranoidCheck(),
                mDBOptions.compression(),
                mDBOptions.filterPolicy());
        retain();
    }

    @Override
    public boolean isOpen() throws KCDBException
    {
        return !(getPtr() == 0);
    }

    @Override
    public void close()
    {
        super.close();
    }

    @Override
    protected void releaseNativeObject(long ptr)
    {
        nativeClose(ptr);

        if (mDestroyOnClose)
        {
            destroy(mPath);
        }
    }

    @Override
    public void destroy() throws KCDBException
    {
        mDestroyOnClose = true;
        if (getPtr() == 0)
        {
            destroy(mPath);
        }
    }

    @Override
    public KCSnapshot createSnapshot() throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        retain();
        return new KCSnapshot(nativeGetSnapshot(mPtr), this)
        {
            @Override
            protected void releaseNativeObject(long ptr)
            {
                nativeReleaseSnapshot(KCDBNative.this.getPtr(), getPtr());
                KCDBNative.this.release();
            }
        };
    }


    // ***********************
    // *       CREATE
    // ***********************

    @Override
    public void put(byte[] aKey, byte[] aValue, boolean aSync) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        if (aKey == null)
        {
            throw new NullPointerException("key");
        }
        if (aValue == null)
        {
            throw new NullPointerException("value");
        }

        nativePut(mPtr, aKey, aValue, aSync);
    }

    @Override
    public void put(byte[] aKey, byte[] aValue) throws KCDBException
    {
        put(aKey, aValue, false);
    }

    @Override
    public void put(String aKey, byte[] aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkArgs(aKey, aValue);
        nativePut(mPtr, aKey, aValue);
    }

    @Override
    public void putString(String aKey, String aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkArgs(aKey, aValue);
        nativePut(mPtr, aKey, aValue);
    }

    @Override
    public void putDBObject(String aKey, KCDBObject aValue) throws KCDBException
    {
        checkArgs(aKey, aValue);
        byte[] bytes = aValue.toBytes();
        if (bytes != null && bytes.length > 0)
        {
            nativePut(mPtr, aKey, bytes);
        }
    }

    @Override
    public void putShort(String aKey, short aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        nativePut(mPtr, aKey, aValue);
    }

    @Override
    public void putInt(String aKey, int aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        nativePut(mPtr, aKey, aValue);
    }

    @Override
    public void putBoolean(String aKey, boolean aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        nativePut(mPtr, aKey, aValue);
    }

    @Override
    public void putDouble(String aKey, double aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        nativePut(mPtr, aKey, aValue);
    }

    @Override
    public void putFloat(String aKey, float aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        nativePut(mPtr, aKey, aValue);
    }

    @Override
    public void putLong(String aKey, long aValue) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        nativePut(mPtr, aKey, aValue);
    }

    // ***********************
    // *      DELETE
    // ***********************
    @Override
    public void remove(byte[] aKey, boolean aSync) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        if (aKey == null)
        {
            throw new NullPointerException();
        }

        nativeDelete(mPtr, aKey, aSync);
    }

    @Override
    public void remove(byte[] aKey) throws KCDBException
    {
        remove(aKey, false);
    }

    @Override
    public void remove(String aKey, boolean aSync) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        nativeDelete(mPtr, aKey, aSync);
    }
    @Override
    public void remove(String aKey) throws KCDBException
    {
        remove(aKey, false);
    }

    // ***********************
    // *      WRITE
    // ***********************

    @Override
    public KCWriteBatch createWritebatch()
    {
        return new KCWriteBatch(this);
    }

    @Override
    public void write(KCWriteBatch aWriteBatch, boolean aSync) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        if (aWriteBatch == null)
        {
            throw new KCNullPointerException();
        }

        nativeWrite(mPtr, aWriteBatch.getPtr(), aSync);
    }

    @Override
    public void write(KCWriteBatch aWriteBatch) throws KCDBException
    {
        write(aWriteBatch, false);
    }

    // ***********************
    // *       RETRIEVE
    // ***********************
    @Override
    public byte[] get(KCSnapshot snapshot, byte[] key) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        if (key == null)
        {
            throw new KCNullPointerException();
        }

        return nativeGet(mPtr, snapshot != null ? snapshot.getPtr() : 0, key);
    }

    @Override
    public byte[] get(KCSnapshot snapshot, ByteBuffer key)throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        if (key == null)
        {
            throw new KCNullPointerException();
        }

        return nativeGet(mPtr, snapshot != null ? snapshot.getPtr() : 0, key);
    }

    @Override
    public byte[] get(KCSnapshot aSnapshot, String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGet(mPtr, aSnapshot != null ? aSnapshot.getPtr() : 0, aKey);
    }

    @Override
    public byte[] get(byte[] key) throws KCDBException
    {
        return get(null, key);
    }
    @Override
    public byte[] get(ByteBuffer key) throws KCDBException
    {
        return get(null, key);
    }

    @Override
    public byte[] get(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetBytes(mPtr, aKey);
    }

    @Override
    public String getString(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetString(mPtr, aKey);
    }

    @Override
    public short getShort(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetShort(mPtr, aKey);
    }

    @Override
    public int getInt(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetInt(mPtr, aKey);
    }

    @Override
    public boolean getBoolean(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetBoolean(mPtr, aKey);
    }

    @Override
    public double getDouble(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetDouble(mPtr, aKey);
    }

    @Override
    public float getFloat(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetFloat(mPtr, aKey);
    }

    @Override
    public long getLong(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeGetLong(mPtr, aKey);
    }

    //****************************
    //*      KEYS OPERATIONS
    //****************************
    @Override
    public boolean exists(String aKey) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkKey(aKey);
        return nativeExists(mPtr, aKey);
    }

    @Override
    public String[] findKeys(String prefix) throws KCDBException
    {
        return findKeys(prefix, 0, LIMIT_MAX);
    }

    @Override
    public String[] findKeys(String prefix, int offset) throws KCDBException
    {
        return findKeys(prefix, offset, LIMIT_MAX);
    }

    @Override
    public String[] findKeys(String prefix, int offset, int limit) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkPrefix(prefix);
        checkOffsetLimit(offset, limit);

        return nativeFindKeys(mPtr,prefix, offset, limit);
    }

    @Override
    public int countKeys(String prefix) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkPrefix(prefix);
        return nativeCountKeys(mPtr, prefix);
    }

    @Override
    public String[] findKeysBetween(String startPrefix, String endPrefix) throws KCDBException
    {
        return findKeysBetween(startPrefix, endPrefix, 0, LIMIT_MAX);
    }

    @Override
    public String[] findKeysBetween(String startPrefix, String endPrefix, int offset) throws KCDBException
    {
        return findKeysBetween(startPrefix, endPrefix, offset, LIMIT_MAX);
    }

    @Override
    public String[] findKeysBetween(String startPrefix, String endPrefix, int offset, int limit) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkRange(startPrefix, endPrefix);
        checkOffsetLimit(offset, limit);

        return nativeFindKeysBetween(mPtr, startPrefix, endPrefix, offset, limit);
    }

    @Override
    public int countKeysBetween(String startPrefix, String endPrefix) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);
        checkRange(startPrefix, endPrefix);
        return nativeCountKeysBetween(mPtr, startPrefix, endPrefix);
    }

    //***********************
    //*      ITERATORS
    //***********************
    @Override
    public KCIterator iterator() throws KCDBException
    {
        return iterator(null);
    }

    @Override
    public KCIterator iterator(final KCSnapshot aSnapshot) throws KCDBException
    {
        return iterator(aSnapshot, true);
    }
    @Override
    public KCIterator iterator(final KCSnapshot aSnapshot, boolean aFillCache) throws KCDBException
    {
        assertNativePtr(ASSERT_DB_MSG);

        retain();

        if (aSnapshot != null)
        {
            aSnapshot.retain();
        }

        return new KCIterator(nativeIterator(mPtr, aSnapshot != null ? aSnapshot.getPtr() : 0, aFillCache))
        {
            @Override
            protected void releaseNativeObject(long ptr)
            {
                super.releaseNativeObject(ptr);
                if (aSnapshot != null)
                {
                    aSnapshot.release();
                }

                KCDBNative.this.release();
            }
        };
    }

    @Override
    public byte[] getPropertyBytes(byte[] aKey) throws KCDBException
    {
        if (aKey == null)
        {
            throw new IllegalArgumentException("Key must not be null.");
        }
        synchronized (this)
        {
            assertNativePtr(ASSERT_DB_MSG);
            return nativeGetProperty(mPtr, aKey);
        }
    }

    @Override
    public void repairDB() throws KCDBException
    {
        nativeRepair(mPath.getAbsolutePath());
    }

    public static void repairDB(File aDBPath) throws KCDBException
    {
        if (aDBPath == null)
        {
            throw new IllegalArgumentException("Key must not be null.");
        }
        nativeRepair(aDBPath.getAbsolutePath());
    }

    // ***********************
    // *      UTILS
    // ***********************

    private void checkArgs(String key, Object value) throws KCDBException
    {
        checkArgNotEmpty(key, "Key must not be empty");

        if (null == value)
        {
            throw new KCDBException("Value must not be empty");
        }
    }

    private void checkPrefix(String prefix) throws KCDBException
    {
        checkArgNotEmpty(prefix, "Starting prefix must not be empty");
    }

    private void checkRange(String startPrefix, String endPrefix) throws KCDBException
    {
        checkArgNotEmpty(startPrefix, "Starting prefix must not be empty");
        checkArgNotEmpty(startPrefix, "Ending prefix must not be empty");
    }

    private void checkKey(String key) throws KCDBException
    {
        checkArgNotEmpty(key, "Key must not be empty");
    }

    private void checkArgNotEmpty(String arg, String errorMsg) throws KCDBException
    {
        if (TextUtils.isEmpty(arg))
        {
            throw new KCDBException(errorMsg);
        }
    }

    private void checkOffsetLimit(int offset, int limit) throws KCDBException
    {
        if (offset < 0)
        {
            throw new KCDBException("Offset must not be negative");
        }
        if (limit <= 0)
        {
            throw new KCDBException("Limit must not be 0 or negative");
        }
    }


    public static void destroy(File path)
    {
        nativeDestroy(path.getAbsolutePath());
    }

    private static native long nativeOpen(String dbpath,boolean createIfMissing, int cacheSize, int blockSize, int writeBufferSize, boolean errorIfExists, boolean paranoidCheck, boolean compression, int filterPolicy) throws KCDBException;
    private static native void nativeClose(long dbPtr);

    private static native void nativePut(long dbPtr, byte[] key, byte[] value, boolean sync) throws KCDBException;
    private static native void nativePut(long dbPtr, String key, byte[] value) throws KCDBException;
    private static native void nativePut(long dbPtr, String key, String value) throws KCDBException;
    private static native void nativePut(long dbPtr, String key, short value) throws KCDBException;
    private static native void nativePut(long dbPtr, String key, int value) throws KCDBException;
    private static native void nativePut(long dbPtr, String key, boolean value);
    private static native void nativePut(long dbPtr, String key, double value);
    private static native void nativePut(long dbPtr, String key, float value);
    private static native void nativePut(long dbPtr, String key, long value);

    private static native void nativeDelete(long dbPtr, byte[] key, boolean sync);
    private static native void nativeDelete(long dbPtr, String key, boolean sync);

    private static native void nativeWrite(long dbPtr, long batchPtr,boolean sync);

    private static native byte[] nativeGet(long dbPtr, long snapshotPtr, byte[] key);
    private static native byte[] nativeGet(long dbPtr, long snapshotPtr, ByteBuffer key);
    private static native byte[] nativeGet(long dbPtr, long snapshotPtr, String key);
    private static native byte[] nativeGetBytes(long dbPtr, String key);
    private static native String nativeGetString(long dbPtr, String key);
    private static native short nativeGetShort(long dbPtr, String key);
    private static native int nativeGetInt(long dbPtr, String key);
    private static native boolean nativeGetBoolean(long dbPtr, String key);
    private static native double nativeGetDouble(long dbPtr, String key);
    private static native float nativeGetFloat(long dbPtr, String key);
    private static native long nativeGetLong(long dbPtr, String key);

    private static native boolean nativeExists(long dbPtr, String key);

    private native String[] nativeFindKeys(long dbPtr, String prefix, int offset, int limit) throws KCDBException;
    private native int nativeCountKeys(long dbPtr, String prefix) throws KCDBException;
    private native String[] nativeFindKeysBetween(long dbPtr, String startPrefix, String endPrefix, int offset, int limit) throws KCDBException;
    private native int nativeCountKeysBetween(long dbPtr, String startPrefix, String endPrefix) throws KCDBException;

    private static native long nativeIterator(long dbPtr, long snapshotPtr, boolean fillCache);

    //not use
    native long nativeFindKeysIterator(long dbPtr, String prefix, boolean reverse) throws KCDBException;


    private static native void nativeDestroy(String dbpath);
    private static native long nativeGetSnapshot(long dbPtr);
    private static native void nativeReleaseSnapshot(long dbPtr, long snapshotPtr);

    /**
     * Natively gets DB property. Pointer is unchecked.
     */
    private static native byte[] nativeGetProperty(long dbPtr, byte[] key);
    private static native void nativeRepair(String path) throws KCDBException;

    static
    {
        System.loadLibrary(LIB_NAME);
    }
}
