package com.kercer.kerdb.example;

import android.os.Parcel;
import android.os.Parcelable;

public class KCPageInfo implements Parcelable
{
    public static final Creator<KCPageInfo> CREATOR = new Creator<KCPageInfo>()
    {
        @Override
        public KCPageInfo createFromParcel(Parcel parcel)
        {
            return new KCPageInfo(parcel);
        }

        @Override
        public KCPageInfo[] newArray(int i)
        {
            return new KCPageInfo[i];
        }
    };
    private String name;
    private Class<? extends KCBaseActivity> activityClazz;

    public KCPageInfo(String name, Class<? extends KCBaseActivity> activityClazz)
    {
        this.name = name;
        this.activityClazz = activityClazz;
    }

    private KCPageInfo(Parcel in)
    {
        name = in.readString();
    }

    @Override
    public int describeContents()
    {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel parcel, int i)
    {
        parcel.writeString(name);
    }

    // Getters/Setters
    public String getName()
    {
        return name;
    }

    public Class<? extends KCBaseActivity> getActivity()
    {
        return activityClazz;
    }

}
