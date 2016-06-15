package com.kercer.kerdb;

import android.content.Context;

import com.kercer.kerdb.jnibridge.exception.KCDBException;
import com.kercer.kerdb.jnibridge.KCDBNative;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KerDB
{
    private final static String DEFAULT_DBNAME = "kerdb";
    private static Map<String, KCDB> mDBMap = new HashMap<>();

    /**
     * Return the Database with the given folder and name, if it doesn't exist create it
     *
     * @param aDir the folder of the db file will be stored
     * @param aDBName database file name
     * @return Database handler {@link com.kercer.kerdb.KCDB}
     * @throws KCDBException
     */
    public static KCDB open(File aDir, String aDBName) throws KCDBException
    {
        return open(aDir, aDBName, null);
    }
    public static synchronized KCDB open(File aDir, String aDBName, KCDBOptions aOptions) throws KCDBException
    {
        File dbFile = new File(aDir, aDBName);
        if (!dbFile.exists())
        {
            dbFile.mkdirs();
        }

        String keyDB = dbFile.getAbsolutePath();
        KCDBNative db =(KCDBNative) mDBMap.get(keyDB);
        if (db == null)
        {
            db = new KCDBNative(dbFile, aOptions);
            db.open();
            mDBMap.put(keyDB, db);
        }
        else
        {
            long ptr = db.getPtr();
            if (ptr == 0)
            {
                db.open();
            }
        }

        return db;
    }

    /**
     * Return the Database with the given path, if it doesn't exist create it
     *
     * @param aDBPath the folder of the db file will be stored
     * @return Database handler {@link com.kercer.kerdb.KCDB}
     * @throws KCDBException
     */
    public static KCDB open(File aDBPath) throws KCDBException
    {
        return open(aDBPath, (KCDBOptions)null);
    }
    public static KCDB open(File aDBPath, KCDBOptions aOptions) throws KCDBException
    {
        String name = aDBPath.getName();
        File fileParent = aDBPath.getParentFile();
        return open(fileParent, name, aOptions);
    }

    /**
     * Return the Database with the given name, if it doesn't exist create it
     *
     * @param ctx    context
     * @param dbName database file name
     * @return Database handler {@link com.kercer.kerdb.KCDB}
     * @throws KCDBException
     */
    public static KCDB open(Context ctx, String dbName) throws KCDBException
    {
        return open(ctx, dbName, null);
    }

    public static KCDB open(Context ctx, String dbName, KCDBOptions aOptions) throws KCDBException
    {
        return open(ctx.getFilesDir(), dbName, aOptions);
    }

    /**
     * Return the Database with the default name {@link KerDB#DEFAULT_DBNAME}, if it doesn't exist create it
     *
     * @param ctx context
     * @return Database handler {@link com.kercer.kerdb.KCDB}
     * @throws KCDBException
     */
    public static KCDB openDefaultDB(Context ctx) throws KCDBException
    {
        return openDefaultDB(ctx, null);
    }
    public static KCDB openDefaultDB(Context ctx, KCDBOptions aOptions) throws KCDBException
    {
        return open(ctx, DEFAULT_DBNAME, aOptions);
    }

    /**
     * force close All DB, if the DB is open
     * @throws KCDBException
     */
    public static synchronized void closeAllDB() throws KCDBException
    {
        for (KCDB db : mDBMap.values())
        {
            if (db != null && db.isOpen()) db.forceClose();
        }
        mDBMap.clear();
    }

    /**
     * If a DB cannot be opened, you may attempt to call this method to resurrect as much of the contents of the
     * database as possible. Some data may be lost, so be careful when calling this function on a database that contains
     * important information.
     *
     * @throws KCDBException
     */
    public static synchronized void repairDB(File aDBPath) throws KCDBException
    {
        KCDBNative.repairDB(aDBPath);
    }

}

