package com.kercer.kerdb;

import android.content.Context;

import com.kercer.kerdb.jnibridge.KCDBException;
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
        File dbFile = new File(aDir, aDBName);
        if (!dbFile.exists())
        {
            dbFile.mkdirs();
        }

        String keyDB = dbFile.getAbsolutePath();
        KCDB db = mDBMap.get(keyDB);
        if (db == null)
        {
            db = new KCDBNative(dbFile);
            db.open();
            mDBMap.put(keyDB, db);
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
        String name = aDBPath.getName();
        File fileParent = aDBPath.getParentFile();
        return open(fileParent, name);
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
        return open(ctx.getFilesDir(), dbName);
    }

    /**
     * Return the Database with the default name {@link KerDB#DEFAULT_DBNAME}, if it doesn't exist create it
     *
     * @param ctx context
     * @return Database handler {@link com.kercer.kerdb.KCDB}
     * @throws KCDBException
     */
    public static KCDB open(Context ctx) throws KCDBException
    {
        return open(ctx, DEFAULT_DBNAME);
    }

    public static void closeAllDB() throws KCDBException
    {
        for (KCDB db : mDBMap.values())
        {
            if (db != null) db.close();
        }
        mDBMap.clear();
    }

}

