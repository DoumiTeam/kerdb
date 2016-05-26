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

    public KCDBOptions()
    {
        createIfMissing = true;
    }

    public boolean createIfMissing()
    {
        return createIfMissing;
    }

    public KCDBOptions createIfMissing(boolean createIfMissing)
    {
        this.createIfMissing = createIfMissing;

        return this;
    }

    public int cacheSize()
    {
        return cacheSize;
    }

    public KCDBOptions cacheSize(int cacheSize) {
        this.cacheSize = Math.abs(cacheSize);

        return this;
    }

    public int blockSize()
    {
        return this.blockSize;
    }

    public KCDBOptions blockSize(int blockSize)
    {
        this.blockSize = Math.abs(blockSize);

        return this;
    }

    public int writeBufferSize()
    {
        return writeBufferSize;
    }

    public KCDBOptions writeBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;

        return this;
    }
}
