package com.kercer.kerdb;

import com.kercer.kerdb.jnibridge.KCDBException;
import com.kercer.kerdb.jnibridge.KCIterator;
import com.kercer.kerdb.jnibridge.KCSnapshot;
import com.kercer.kerdb.jnibridge.KCWriteBatch;

import java.nio.ByteBuffer;

public interface KCDB
{
    //******************************************************************************************************************
    //*      DB MANAGEMENT
    //******************************************************************************************************************

    /**
     * Open database
     *
     * @throws KCDBException
     */
    void open() throws KCDBException;

    /**
     * Checks if database is open.
     *
     * @return {@code true} if database is open.
     */
    boolean isOpen() throws KCDBException;

    /**
     * Closes database.
     *
     * @throws KCDBException
     */
    void close() throws KCDBException;

    /**
     * Destroys database
     *
     * @throws KCDBException
     */
    void destroy() throws KCDBException;

    /**
     * Return an KCSnapshot instance for this database
     *
     * KCSnapshot are a way to "freeze" the state of the database. Write operation applied to the database after the
     * snapshot was taken do not affect the snapshot. Most *read* methods available in the KCDB class are also
     * available in the KCSnapshot class.
     *
     * @return
     * @throws KCDBException
     */
    KCSnapshot createSnapshot() throws KCDBException;


    //******************************************************************************************************************
    //*      CREATE
    //******************************************************************************************************************

    /**
     * Puts the byte array data for the key which the type is byte array
     *
     * @param aKey   not null
     * @param aValue not null
     * @throws KCDBException if the key or data is null.
     */
    void put(byte[] aKey, byte[] aValue) throws KCDBException;

    void put(byte[] aKey, byte[] aValue, boolean aSync) throws KCDBException;

    /**
     * Puts the byte array data for the key.
     *
     * @param aKey   not null.
     * @param aValue not null.
     * @throws KCDBException if the key or data is null.
     */
    void put(String aKey, byte[] aValue) throws KCDBException;

    /**
     * Puts the {@link String} value for the key.
     *
     * @param aKey   not null.
     * @param aValue not null.
     * @throws KCDBException if the key or value is null.
     */
    void putString(String aKey, String aValue) throws KCDBException;


    /**
     * Puts the {@link KCDBObject} for the key.
     *
     * @param aKey   not null.
     * @param aValue not null.
     * @throws KCDBException if the key or object is null.
     */
    void putDBObject(String aKey, KCDBObject aValue) throws KCDBException;


    /**
     * Puts the primitive integer for the key.
     *
     * @param aKey   not null.
     * @param aValue
     * @throws KCDBException if the key is null.
     */
    void putInt(String aKey, int aValue) throws KCDBException;

    /**
     * Puts the primitive short for the key.
     *
     * @param aKey   not null.
     * @param aValue
     * @throws KCDBException if the key is null.
     */
    void putShort(String aKey, short aValue) throws KCDBException;

    /**
     * Puts the primitive boolean for the key.
     *
     * @param aKey   not null.
     * @param aValue
     * @throws KCDBException if the key is null.
     */
    void putBoolean(String aKey, boolean aValue) throws KCDBException;

    /**
     * Puts the primitive double for the key.
     *
     * @param aKey   not null.
     * @param aValue
     * @throws KCDBException if the key is null.
     */
    void putDouble(String aKey, double aValue) throws KCDBException;

    /**
     * Puts the primitive float for the key.
     *
     * @param aKey   not null.
     * @param aValue
     * @throws KCDBException if the key is null.
     */
    void putFloat(String aKey, float aValue) throws KCDBException;

    /**
     * Puts the primitive long for the key.
     *
     * @param aKey   not null.
     * @param aValue
     * @throws KCDBException if the key is null.
     */
    void putLong(String aKey, long aValue) throws KCDBException;

    //******************************************************************************************************************
    //*      REMOVE
    //******************************************************************************************************************

    /**
     * Deletes value for the key.
     *
     * @param aKey not null.
     * @throws KCDBException if the key is null.
     */
    void remove(byte[] aKey) throws KCDBException;

    void remove(byte[] aKey, boolean aSync) throws KCDBException;

    /**
     * Deletes value for the key.
     *
     * @param aKey not null.
     * @throws KCDBException if the key is null.
     */
    void remove(String aKey) throws KCDBException;

    void remove(String aKey, boolean aSync) throws KCDBException;

    //******************************************************************************************************************
    //*      WRITE
    //******************************************************************************************************************


    /**
     * Return an retained KCWritebatch instance for this database
     */
    KCWriteBatch createWritebatch();

    /**
     * Apply the operations from a writebatch into the current database
     *
     * @param aWriteBatch
     * @throws KCDBException
     */
    void write(KCWriteBatch aWriteBatch) throws KCDBException;

    void write(KCWriteBatch aWriteBatch, boolean aSync) throws KCDBException;

    //******************************************************************************************************************
    //*      RETRIEVE
    //******************************************************************************************************************

    /**
     * get bytes with key bytes and snapshot
     *
     * @param aSnapshot snapshot
     * @param aKey      key bytes
     * @return bytes
     * @throws KCDBException exception
     */
    byte[] get(KCSnapshot aSnapshot, byte[] aKey) throws KCDBException;

    /**
     * get bytes with snapshot
     *
     * @param aSnapshot snapshot
     * @param aKey      key bytes
     * @return bytes
     * @throws KCDBException exception
     */
    byte[] get(KCSnapshot aSnapshot, ByteBuffer aKey) throws KCDBException;

    /**
     * get bytes with snapshot
     *
     * @param aSnapshot snapshot
     * @param aKey      key bytes
     * @return bytes
     * @throws KCDBException exception
     */
    byte[] get(KCSnapshot aSnapshot, String aKey) throws KCDBException;

    /**
     * get bytes
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    byte[] get(byte[] aKey) throws KCDBException;

    /**
     * get bytes
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    byte[] get(ByteBuffer aKey) throws KCDBException;

    /**
     * get bytes
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    byte[] get(String aKey) throws KCDBException;

    /**
     * get string
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    String getString(String aKey) throws KCDBException;

    /**
     * get short
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    short getShort(String aKey) throws KCDBException;

    /**
     * get int
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    int getInt(String aKey) throws KCDBException;

    /**
     * get boolean
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    boolean getBoolean(String aKey) throws KCDBException;

    /**
     * get double
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    double getDouble(String aKey) throws KCDBException;

    /**
     * get float
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    float getFloat(String aKey) throws KCDBException;

    /**
     * get long
     *
     * @param aKey
     * @return
     * @throws KCDBException
     */
    long getLong(String aKey) throws KCDBException;

    //******************************************************************************************************************
    //*      KEYS OPERATIONS
    //******************************************************************************************************************
    boolean exists(String key) throws KCDBException;

    String[] findKeys(String aPrefix) throws KCDBException;
    String[] findKeys(String aPrefix, int aOffset) throws KCDBException;
    String[] findKeys(String aPrefix, int aOffset, int aLimit) throws KCDBException;
    int countKeys(String aPrefix) throws KCDBException;

    String[] findKeysBetween(String aStartPrefix, String aEndPrefix) throws KCDBException;
    String[] findKeysBetween(String aStartPrefix, String aEndPrefix, int aOffset) throws KCDBException;
    String[] findKeysBetween(String aStartPrefix, String aEndPrefix, int aOffset, int aLimit) throws KCDBException;
    int countKeysBetween(String aStartPrefix, String aEndPrefix) throws KCDBException;

    //******************************************************************************************************************
    //*      ITERATORS
    //******************************************************************************************************************
    KCIterator iterator() throws KCDBException;
    KCIterator iterator(final KCSnapshot aSnapshot) throws KCDBException;
    KCIterator iterator(final KCSnapshot aSnapshot, boolean aFillCache) throws KCDBException;
    KCIterator iterator(String aStartPrefix, String aEndPrefix, boolean aReverse) throws KCDBException;

    byte[] getPropertyBytes(byte[] key);

    /**
     * If a DB cannot be opened, you may attempt to call this method to resurrect as much of the contents of the
     * database as possible. Some data may be lost, so be careful when calling this function on a database that contains
     * important information.
     *
     * @throws KCDBException
     */
    void repairDB() throws KCDBException;

}
