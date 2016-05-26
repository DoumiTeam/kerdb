package com.kercer.kerdb.example;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.widget.ExpandableListView;

import com.kercer.kerdb.KCDB;
import com.kercer.kerdb.KerDB;
import com.kercer.kerdb.jnibridge.KCDBException;

import java.io.File;
import java.util.ArrayList;
import java.util.TreeMap;


public class MainActivity extends ActionBarActivity implements KCExpandableListAdapter.OnSnippetClicked
{


    void testMultipleDB()
    {
        try
        {
            String dbname = "db_zihong";
            File dbPath  = new File("data/data/" + getPackageName() + "/databases/" + dbname) ;
            KCDB db1 = KerDB.open(dbPath);
            db1.putString("key", "zihong");
            String v = db1.getString("aa");
            Log.i("kerdb", v);


            String dbname2 = "db_zihong2";
            File dbPath2  = new File("data/data/" + getPackageName() + "/databases/" + dbname2) ;
            KCDB db2 = KerDB.open(dbPath2);
            db2.putString("key", "zihong2222222");
            String v2 = db2.getString("aa");
            Log.i("kerdb", v2);
        }
        catch (KCDBException e)
        {
            e.printStackTrace();
        }

    }

    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);

        testMultipleDB();

        TreeMap<String, ArrayList<KCPageInfo>> snippets = KCPageInfoFactory.INSTANCE.getSnippets();

        ExpandableListView expandableListView = (ExpandableListView) findViewById(R.id.categories);
        KCExpandableListAdapter adapter = new KCExpandableListAdapter(LayoutInflater.from(this),//
                new ArrayList<>(snippets.keySet()), snippets, this);
        expandableListView.setAdapter(adapter);
        expandableListView.setOnChildClickListener(adapter);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu)
    {
        MenuInflater inflater = getMenuInflater();
        inflater.inflate(R.menu.about, menu);
        return super.onCreateOptionsMenu(menu);
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item)
    {
        switch (item.getItemId())
        {
            case R.id.action_about:
                //                openAbout();
                return true;
            default:
                return super.onOptionsItemSelected(item);
        }
    }

    @Override
    public void onClick(KCPageInfo aPageInfo)
    {
        Intent intent = new Intent(this, aPageInfo.getActivity());
        intent.putExtra(KCBaseActivity.SNIPPET_ARG, aPageInfo);
        startActivity(intent);
    }

}