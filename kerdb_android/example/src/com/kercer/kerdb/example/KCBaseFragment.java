package com.kercer.kerdb.example;

import android.support.annotation.NonNull;
import android.support.v4.app.Fragment;

import com.kercer.kerdb.KCDB;
import com.kercer.kerdb.KerDB;
import com.kercer.kerdb.jnibridge.KCDBException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class KCBaseFragment extends Fragment
{
    protected ExecutorService mExecutor;
    protected KCDB mDB;

    @Override
    public void onStart()
    {
        super.onStart();
        mExecutor = Executors.newSingleThreadExecutor(new ThreadFactory()
        {
            @Override
            public Thread newThread(@NonNull Runnable runnable)
            {
                Thread thread = new Thread(runnable);
                thread.setPriority(android.os.Process.THREAD_PRIORITY_BACKGROUND);
                return thread;
            }
        });

        try
        {
            mDB = KerDB.open(getActivity(), getClass().getSimpleName());

        }
        catch (KCDBException e)
        {
            e.printStackTrace();
            throw new IllegalStateException("Can't create database");
        }
    }

    @Override
    public void onStop()
    {
        super.onStop();
         if (null != mExecutor)
        {
            mExecutor.shutdownNow();
        }

        if (null != mDB)
        {
            try
            {
                mDB.close();
            }
            catch (KCDBException e)
            {
                e.printStackTrace();
            }
        }
    }
}
