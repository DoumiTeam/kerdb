package com.kercer.kerdb.jnibridge;

import com.kercer.kerdb.KCDB;

/**
 * Created by zihong on 16/5/18.
 */
public abstract class KCSnapshot extends KCNativeObject
{
    private KCDB mDB;
    protected KCSnapshot(long aPtr, KCDB aDB)
    {
        super(aPtr);
        mDB = aDB;
    }

    public KCDB DB()
    {
        return mDB;
    }
}
