## Insert general types
```java
	kerdb.put(byteArray, byteArray);
	kerdb.put("string", byteArray);
	kerdb.putString("quote", "bazinga!");
	kerdb.putShort("myshort", (short)32768);
	kerdb.putInt("max_int", Integer.MAX_VALUE);
	kerdb.putLong("max_long", Long.MAX_VALUE);
	kerdb.putDouble("max_double", Double.MAX_VALUE);
	kerdb.putFloat("myfloat", 10.30f);
	kerdb.putBoolean("myboolean", true);
```
## Write to db with write batch
```java
	kerdb.write(KCWriteBatch batch);
```
## Read general types
```java
	byte[] get(KCSnapshot snapshot, byte[] key)
	byte[] get(KCSnapshot snapshot, ByteBuffer key)
	byte[] get(KCSnapshot aSnapshot, String aKey)
	byte[] get(byte[] key)
	byte[] get(ByteBuffer key)
	byte[] get(String key)
	short getShort(String key) 
	String getString(String key)
	int getInt(String key)
	boolean getBoolean(String key) 
	double getDouble(String key) 
	float getFloat(String key)  
	long getLong(String key) 
```
## Check Key existence
```java
     boolean isKeyExists = kerdb.exists("key");
```
## Delete Key
```java
	kerdb.del("key");
	kerdb.del(byteArray);
```
