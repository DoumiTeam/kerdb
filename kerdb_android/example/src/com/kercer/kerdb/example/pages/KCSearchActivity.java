package com.kercer.kerdb.example.pages;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import com.kercer.kerdb.example.KCBaseActivity;
import com.kercer.kerdb.example.KCBaseFragment;
import com.kercer.kerdb.jnibridge.KCDBException;

import com.kercer.kerdb.example.R;
import java.lang.ref.WeakReference;

public class KCSearchActivity extends KCBaseActivity
{

    @Override
    public Fragment getExecutionFragment() {
        return new KCFindKeysFragment();
    }

    public static class KCFindKeysFragment extends KCBaseFragment implements View.OnClickListener {
        private WeakReference<EditText> mPrefix, mRangeFrom, mRangeTo;
        private Button mBtnPrefix, mBtnRange;
        private WeakReference<TextView> mResult;

        @Override
        public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
            View view = inflater.inflate(R.layout.search_operations, container, false);
            mPrefix = new WeakReference<>((EditText) view.findViewById(R.id.prefix));
            mRangeFrom = new WeakReference<>((EditText) view.findViewById(R.id.rangeFrom));
            mRangeTo = new WeakReference<>((EditText) view.findViewById(R.id.rangeTo));
            mResult = new WeakReference<>((TextView) view.findViewById(R.id.result));

            mBtnPrefix = (Button) view.findViewById(R.id.btnPrefix);
            mBtnRange = (Button) view.findViewById(R.id.btnRange);
            mBtnPrefix.setOnClickListener(this);
            mBtnRange.setOnClickListener(this);

            return view;
        }

        @Override
        public void onStart() {
            super.onStart();
            populateSampleData();
        }

        @Override
        public void onClick(View view) {
            switch (view.getId()) {
                case R.id.btnPrefix: {
                    searchByPrefix();
                    break;
                }

                case R.id.btnRange: {
                    searchByRange();
                    break;
                }
            }
        }

        private void populateSampleData() {
            // populate db with the example data
            mExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        mDB.putString("android:03", "Cupcake");
                        mDB.putString("android:04", "Donut");
                        mDB.putString("android:05", "Eclair");
                        mDB.putString("android:08", "Froyo");
                        mDB.putString("android:09", "Gingerbread");
                        mDB.putString("android:11", "Honeycomb");
                        mDB.putString("android:14", "Ice Cream Sandwich");
                        mDB.putString("android:16", "Jelly Bean");
                        mDB.putString("android:19", "KitKat");

                    } catch (NullPointerException | KCDBException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        // database operations //

        private void searchByPrefix() {
            mExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        String prefix = mPrefix.get().getText().toString();

                        final StringBuilder result = new StringBuilder();

                        String[] keys = mDB.findKeys(prefix);

                        if (null != keys && keys.length > 0) {
                            for (String key : keys) {
                                result.append(mDB.getString(key)).append("\n");
                            }
                        }

                        mResult.get().post(new Runnable() {
                            @Override
                            public void run() {
                                mResult.get().setText(result.toString());
                            }
                        });
                    } catch (NullPointerException | KCDBException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void searchByRange() {
            mExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        String from = mRangeFrom.get().getText().toString();
                        String to = mRangeTo.get().getText().toString();

                        String[] keys = mDB.findKeysBetween(from, to);

                        final StringBuilder result = new StringBuilder();

                        if (null != keys && keys.length > 0) {
                            for (String key : keys) {
                                result.append(mDB.getString(key)).append("\n");
                            }
                        }

                        mResult.get().post(new Runnable() {
                            @Override
                            public void run() {
                                mResult.get().setText(result.toString());
                            }
                        });

                    } catch (NullPointerException | KCDBException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
