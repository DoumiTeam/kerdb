package com.kercer.kerdb;

/**
 * Created by zihong on 16/5/26.
 */
public class KCDBOptions
{
    private boolean createIfMissing;
    private int cacheSize;
    private int blockSize;
    private int writeBufferSize;

    private boolean errorIfExists;
    private boolean paranoidCheck;
    private boolean compression;
    private int filterPolicy;

    public KCDBOptions()
    {
        createIfMissing = true;
        errorIfExists = false;
        paranoidCheck = false;
        compression = true;
        filterPolicy = 0;
        cacheSize = 0;
        blockSize = 0;
        writeBufferSize = 0;
    }

    public boolean createIfMissing()
    {
        return createIfMissing;
    }

    public KCDBOptions createIfMissing(boolean aCreateIfMissing)
    {
        this.createIfMissing = aCreateIfMissing;

        return this;
    }

    public int cacheSize()
    {
        return cacheSize;
    }

    public KCDBOptions cacheSize(int aCacheSize) {
        this.cacheSize = Math.abs(aCacheSize);

        return this;
    }

    public int blockSize()
    {
        return this.blockSize;
    }

    public KCDBOptions blockSize(int aBlockSize)
    {
        this.blockSize = Math.abs(aBlockSize);

        return this;
    }

    public int writeBufferSize()
    {
        return writeBufferSize;
    }

    public KCDBOptions writeBufferSize(int aWriteBufferSize)
    {
        this.writeBufferSize = aWriteBufferSize;

        return this;
    }

    public boolean errorIfExists()
    {
        return errorIfExists;
    }

    public KCDBOptions errorIfExists(boolean aErrorIfExists)
    {
        this.errorIfExists = aErrorIfExists;
        return this;
    }

    public boolean paranoidCheck()
    {
        return paranoidCheck;
    }

    public KCDBOptions paranoidCheck(boolean aParanoidCheck)
    {
        this.paranoidCheck = aParanoidCheck;
        return this;
    }

    public boolean compression()
    {
        return compression;
    }
    public KCDBOptions compression(boolean aCompression)
    {
        this.compression = aCompression;
        return this;
    }

    public int filterPolicy()
    {
        return filterPolicy;
    }
    public KCDBOptions filterPolicy(int aFilterPolicy)
    {
        this.filterPolicy = aFilterPolicy;
        return this;
    }
}
