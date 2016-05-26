package com.kercer.kerdb.example.pages;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;

import com.kercer.kerdb.example.KCBaseActivity;
import com.kercer.kerdb.example.KCBaseFragment;
import com.kercer.kerdb.jnibridge.KCDBException;
import com.kercer.kerdb.example.R;

import java.lang.ref.WeakReference;

public class KCGeneralActivity extends KCBaseActivity
{

    @Override
    public Fragment getExecutionFragment()
    {
        return new KCBuildInTypeFragment();
    }

    public static class KCBuildInTypeFragment extends KCBaseFragment implements View.OnClickListener
    {
        private WeakReference<EditText> mWeakTxtValueString, mWeakTxtKeyString, mWeakTxtValueInt, mWeakTxtKeyInt, mWeakTxtValueDouble, mWeakTxtKeyDouble, mWeakTxtKeyBoolean, mWeakTxtKeyDelete, mWeakTxtKeyCheck, mWeakTxtValueCheck;

        private WeakReference<Spinner> mWeakValueBoolean;

        private Button mBtnStringInsert, mBtnStringGet, mBtnIntInsert, mBtnIntGet, mBtnDoubleInsert, mBtnDoubleGet, mBtnBooleanInsert, mBtnBooleanGet, mBtnCheckKey, mBtnDeleteKey;

        @Override
        public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState)
        {
            View view = inflater.inflate(R.layout.general_operations, container, false);

            mWeakTxtKeyString = new WeakReference<>((EditText) view.findViewById(R.id.txt_key_string));
            mWeakTxtValueString = new WeakReference<>((EditText) view.findViewById(R.id.txt_value_string));

            mWeakTxtKeyInt = new WeakReference<>((EditText) view.findViewById(R.id.txt_key_int));
            mWeakTxtValueInt = new WeakReference<>((EditText) view.findViewById(R.id.txt_value_int));

            mWeakTxtKeyDouble = new WeakReference<>((EditText) view.findViewById(R.id.txt_key_double));
            mWeakTxtValueDouble = new WeakReference<>((EditText) view.findViewById(R.id.txt_value_double));

            mWeakTxtKeyBoolean = new WeakReference<>((EditText) view.findViewById(R.id.txt_key_boolean));
            mWeakValueBoolean = new WeakReference<>((Spinner) view.findViewById(R.id.value_boolean));

            mWeakTxtKeyDelete = new WeakReference<>((EditText) view.findViewById(R.id.txt_key_delete));
            mWeakTxtKeyCheck = new WeakReference<>((EditText) view.findViewById(R.id.txt_key_check));
            mWeakTxtValueCheck = new WeakReference<>((EditText) view.findViewById(R.id.txt_value_check));

            mBtnStringInsert = (Button) view.findViewById(R.id.btn_string_insert);
            mBtnStringGet = (Button) view.findViewById(R.id.btn_string_get);
            mBtnStringInsert.setOnClickListener(this);
            mBtnStringGet.setOnClickListener(this);

            mBtnIntInsert = (Button) view.findViewById(R.id.btn_int_insert);
            mBtnIntGet = (Button) view.findViewById(R.id.btn_int_get);
            mBtnIntInsert.setOnClickListener(this);
            mBtnIntGet.setOnClickListener(this);

            mBtnDoubleInsert = (Button) view.findViewById(R.id.btn_double_insert);
            mBtnDoubleGet = (Button) view.findViewById(R.id.btn_double_get);
            mBtnDoubleInsert.setOnClickListener(this);
            mBtnDoubleGet.setOnClickListener(this);

            mBtnBooleanInsert = (Button) view.findViewById(R.id.btn_boolean_insert);
            mBtnBooleanGet = (Button) view.findViewById(R.id.btn_boolean_get);
            mBtnBooleanInsert.setOnClickListener(this);
            mBtnBooleanGet.setOnClickListener(this);

            mBtnCheckKey = (Button) view.findViewById(R.id.btn_check_key);
            mBtnDeleteKey = (Button) view.findViewById(R.id.btn_delete_key);
            mBtnCheckKey.setOnClickListener(this);
            mBtnDeleteKey.setOnClickListener(this);

            return view;
        }

        @Override
        public void onClick(View view)
        {
            switch (view.getId())
            {
                case R.id.btn_string_insert:
                {
                    putString();
                    break;
                }

                case R.id.btn_string_get:
                {
                    getString();
                    break;
                }

                case R.id.btn_int_insert:
                {
                    putInt();
                    break;
                }

                case R.id.btn_int_get:
                {
                    getInt();
                    break;
                }

                case R.id.btn_double_insert:
                {
                    putDouble();
                    break;
                }

                case R.id.btn_double_get:
                {
                    getDouble();
                    break;
                }

                case R.id.btn_boolean_insert:
                {
                    putBoolean();
                    break;
                }

                case R.id.btn_boolean_get:
                {
                    getBoolean();
                    break;
                }

                case R.id.btn_check_key:
                {
                    checkKey();
                    break;
                }

                case R.id.btn_delete_key:
                {
                    delete();
                    break;
                }
            }
        }


        // database operations //

        private void putString()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyString.get().getText().toString();
                        String value = mWeakTxtValueString.get().getText().toString();

                        mDB.putString(key, value);

                        mWeakTxtValueString.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                // Clear the inserted value, as a visual indicator that the operation succeeded
                                mWeakTxtValueString.get().setText("");
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void getString()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        final String value = mDB.getString(mWeakTxtKeyString.get().getText().toString());

                        mWeakTxtValueString.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                mWeakTxtValueString.get().setText(value);
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void putInt()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyInt.get().getText().toString();

                        int value = Integer.parseInt(mWeakTxtValueInt.get().getText().toString());

                        mDB.putInt(key, value);

                        mWeakTxtValueInt.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                mWeakTxtValueInt.get().setText("");
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void getInt()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyInt.get().getText().toString();

                        final int value = mDB.getInt(key);

                        mWeakTxtValueInt.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                mWeakTxtValueInt.get().setText("" + value);
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void putDouble()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyDouble.get().getText().toString();
                        double value = Double.parseDouble(mWeakTxtValueDouble.get().getText().toString());

                        mDB.putDouble(key, value);

                        mWeakTxtValueDouble.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {

                                mWeakTxtValueDouble.get().setText("");
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void getDouble()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        final double value = mDB.getDouble(mWeakTxtKeyDouble.get().getText().toString());

                        mWeakTxtValueDouble.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                mWeakTxtValueDouble.get().setText("" + value);
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void putBoolean()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyBoolean.get().getText().toString();
                        boolean value = Boolean.parseBoolean(mWeakValueBoolean.get().getSelectedItem().toString());

                        mDB.putBoolean(key, value);

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void getBoolean()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyBoolean.get().getText().toString();

                        final boolean value = mDB.getBoolean(key);

                        mWeakValueBoolean.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                mWeakValueBoolean.get().setSelection(value ? 0 : 1);
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });

        }

        private void checkKey()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyCheck.get().getText().toString();

                        final boolean value = mDB.exists(key);

                        mWeakTxtValueCheck.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                mWeakTxtValueCheck.get().setText(value ? "Exist" : "Does not exist");
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }

        private void delete()
        {
            mExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        String key = mWeakTxtKeyDelete.get().getText().toString();

                        mDB.remove(key);

                        mWeakTxtKeyDelete.get().post(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                mWeakTxtKeyDelete.get().setText("");
                            }
                        });

                    }
                    catch (NullPointerException | KCDBException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
