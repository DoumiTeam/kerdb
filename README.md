
# KerDB 

KerDB can outperform SQLite in read/write operations. 

[![client](http://src.linzihong.com/kerdbsqlite.png)](http://src.linzihong.com/kerdbsqlite.png)

## Usage
### Android
```
    try
    {
        String dbname = "db_zihong";
        File dbPath  = new File("data/data/" + getPackageName() + "/databases/" + dbname) ;
        KCDB db1 = KerDB.open(dbPath);
        db1.putString("key", "zihong1");
        String v = db1.getString("aa");
        Log.i("kerdb", v);


        String dbname2 = "db_zihong2";
        File dbPath2  = new File("data/data/" + getPackageName() + "/databases/" + dbname2) ;
        KCDB db2 = KerDB.open(dbPath2);
        db2.putString("key", "zihong2");
        String v2 = db2.getString("aa");
        Log.i("kerdb", v2);
            
        db1.close();
        db2.close();
    }
    catch (KCDBException e)
    {
        e.printStackTrace();
    }
```

### iOS
```
    KCDB* db = [KerDB openDefaultDB];
    if (db && [db open])
    {
        NSData* dataValue = [@"testdata" dataUsingEncoding:NSUTF8StringEncoding];
        [db put:dataValue key:@"keyString"];
        
        NSData* dataGetValue = [db get:@"keyString" snapshot:nil];
        XCTAssertNotNil(dataGetValue);
        NSString *strGetValue = [[NSString alloc] initWithData:dataGetValue  encoding:NSUTF8StringEncoding];
        XCTAssert([strGetValue isEqualToString:@"testdata"]);
        KCRelease(strGetValue);
        
        NSData* dataKey2 = [@"keyString2" dataUsingEncoding:NSUTF8StringEncoding];
        NSData* dataGetValue2 = [db getWithKeyData:dataKey2 snapshot:nil];
        XCTAssertNil(dataGetValue2);
        
        [db close];
        
    }
```
