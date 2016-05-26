
package com.kercer.kerdb.example;

import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentTransaction;
import android.support.v7.app.ActionBarActivity;


public abstract class KCBaseActivity extends ActionBarActivity
{
    public final static String SNIPPET_ARG = "snippet";

    private KCPageInfo mPageInfo;

    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);

        if (null != savedInstanceState)
        {
            mPageInfo = savedInstanceState.getParcelable(SNIPPET_ARG);

        }
        else if (getIntent().hasExtra(SNIPPET_ARG))
        {
            mPageInfo = getIntent().getParcelableExtra(SNIPPET_ARG);

        }
        else
        {
            throw new IllegalArgumentException("Need a page info instance to work");
        }

        setContentView(R.layout.base_layout);

        setTitle(mPageInfo.getName());

        FragmentTransaction fragmentTransaction = getSupportFragmentManager().beginTransaction();

        Fragment execFrag = getExecutionFragment();
        if (null != execFrag)
        {
            fragmentTransaction.replace(R.id.execution_fragment, execFrag, null);
        }

        fragmentTransaction.commit();
    }

    @Override
    protected void onSaveInstanceState(Bundle outState)
    {
        super.onSaveInstanceState(outState);
        outState.putParcelable(SNIPPET_ARG, mPageInfo);
    }

    protected abstract Fragment getExecutionFragment();

}
