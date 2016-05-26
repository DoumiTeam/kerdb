## Create/Open database
##### Using the default name/location
the Database with the default name "kerdb", if it doesn't exist create it

```java
     KCDB kerdb = KerDB.open(context);
```
##### With a given name
the Database with the given name, if it doesn't exist create it

```java
     KCDB kerdb = KerDB.open(context, "zihong");
```
KerDB use the internal storage to create your database.
It create a directory containing all the necessary files Ex:
``
/data/data/com.kercer/files/mydatabse
``
##### Using a given DB path
```java
	String dbname = "db_zihong";
	File dbPath  = new File("data/data/" + getPackageName() + "/databases/" + dbname) ;
	KCDB db = KerDB.open(dbPath);
```
 you Specify the location of the database (*sdcard*)
 
##### Using a given directory and DB name
the Database with the given folder and name, if it doesn't exist create it

```java
	KCDB kerdb = KerDB.open(File aDir, String aDBName)
```


## Close database
```java
     kerdb.close();
```

## Destroy database
```java
     kerdb.destroy();
```

## The database is Open?
```java
     kerdb.isOpen();
```

## Get Snapshot
```java
     kerdb.getSnapshot();
```