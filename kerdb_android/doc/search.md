## Iterator
```java
KCIterator iterator()
KCIterator iterator(final KCSnapshot snapshot) 
```

## Prefix keys search
```java
kerdb.putString("android:03", "Cupcake"); // adding 0 to maintain lexicographical order
kerdb.putString("android:04", "Donut");
kerdb.putString("android:05", "Eclair");
kerdb.putString("android:08", "Froyo");
kerdb.putString("android:09", "Gingerbread");
kerdb.putString("android:11", "Honeycomb");
kerdb.putString("android:14", "Ice Cream Sandwich");
kerdb.putString("android:16", "Jelly Bean");
kerdb.putString("android:19", "KitKat");

String [] keys = kerdb.findKeys("android");
assert keys.length == 9;

keys = kerdb.findKeys("android:0");
assert keys.length == 5;

assert kerdb.getString(keys[0]).equals("Cupcake");
assert kerdb.getString(keys[1]).equals("Donut");
assert kerdb.getString(keys[2]).equals("Eclair");
assert kerdb.getString(keys[3]).equals("Froyo");
assert kerdb.getString(keys[4]).equals("Gingerbread");

keys = kerdb.findKeys("android:1");
assert keys.length == 4;

assert kerdb.getString(keys[0]).equals("Honeycomb");
assert kerdb.getString(keys[1]).equals("Ice Cream Sandwich");
assert kerdb.getString(keys[2]).equals("Jelly Bean");
assert kerdb.getString(keys[3]).equals("KitKat");


```
## Range keys search

+ both 'FROM' & 'TO' keys exist
```java
keys = kerdb.findKeysBetween("android:08", "android:11");
assertEquals(3, keys.length);
assertEquals("android:08", keys[0]);
assertEquals("android:09", keys[1]);
assertEquals("android:11", keys[2]);
```

+ 'FROM' key exist, but not the `TO
```java
keys = kerdb.findKeysBetween("android:05", "android:10");
assertEquals(3, keys.length);
assertEquals("android:05", keys[0]);
assertEquals("android:08", keys[1]);
assertEquals("android:09", keys[2]);
```

+ 'FROM' key doesn't exist but the 'TO' key do
```java
keys = kerdb.findKeysBetween("android:07", "android:09");
assertEquals(2, keys.length);
assertEquals("android:08", keys[0]);
assertEquals("android:09", keys[1]);
```

+ both 'FROM' & 'TO' keys doesn't exist
```java
keys = kerdb.findKeysBetween("android:13", "android:99");
assertEquals(3, keys.length);
assertEquals("android:14", keys[0]);
assertEquals("android:16", keys[1]);
assertEquals("android:19", keys[2]);
```

