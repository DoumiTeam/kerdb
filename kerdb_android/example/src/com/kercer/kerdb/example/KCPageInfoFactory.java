package com.kercer.kerdb.example;

import com.kercer.kerdb.example.pages.KCGeneralActivity;
import com.kercer.kerdb.example.pages.KCSearchActivity;

import java.util.ArrayList;
import java.util.TreeMap;

public enum KCPageInfoFactory
{
    INSTANCE;
    private final TreeMap<String, ArrayList<KCPageInfo>> snippets;

    private KCPageInfoFactory()
    {
        snippets = new TreeMap<>();

        ArrayList<KCPageInfo> children = new ArrayList<>();

        children.add(new KCPageInfo("[General]",  KCGeneralActivity.class));

        snippets.put("01. Basics", children);


        children = new ArrayList<>();
        children.add(new KCPageInfo("[Prefixes]", KCSearchActivity.class));

        snippets.put("02. Keys Search", children);
    }

    public TreeMap<String, ArrayList<KCPageInfo>> getSnippets()
    {
        return snippets;
    }
}
