//
//  KCDB.m
//  kerdb
//
//  Created by zihong on 16/5/20.
//  Copyright © 2016年 com.kercer. All rights reserved.
//

#import "KCDB.h"
#import "KCCommon.h"

#import <leveldb/db.h>
#import <leveldb/options.h>
#import <leveldb/cache.h>
#import <leveldb/filter_policy.h>
#import <leveldb/write_batch.h>
#include <sstream>
#include <iomanip>
#include <vector>


#define LIMIT_MAX (INT_MAX-8)

#define MaybeAddSnapshotToOptions(_from_, _to_, _snap_) \
    leveldb::ReadOptions __to_;\
    leveldb::ReadOptions * _to_ = &__to_;\
    if (_snap_ != nil) { \
        _to_->fill_cache = _from_.fill_cache; \
        _to_->snapshot = [_snap_ getSnapshot]; \
    } else \
        _to_ = &_from_;

#define SeekToKey(iter, key, _backward_) \
    (key != nil) ? iter->Seek(SliceFromNSStringOrNSData(key)) : \
    _backward_ ? iter->SeekToLast() : iter->SeekToFirst()

#define MoveCursor(_iter_, _backward_) \
    _backward_ ? iter->Prev() : iter->Next()


typedef struct
{
    KCBytes bytes;
    leveldb::Status status;
} KCBytesStatus;


NSString* NSStringFromBytes(KCBytes& aBytes)
{
    NSString* str = [[NSString alloc] initWithBytes:aBytes.data
                                             length:aBytes.length
                                           encoding:NSUTF8StringEncoding];
    KCAutorelease(str);
    
    return str;
}
NSData* NSDataFromBytes(KCBytes& aBytes)
{
    return [NSData dataWithBytes:aBytes.data length:aBytes.length];
}
KCBytes BytesFromNSString(NSString* aString)
{
    KCBytes bytes =(KCBytes){.data = [aString cStringUsingEncoding:NSUTF8StringEncoding],
                                .length = [aString lengthOfBytesUsingEncoding:NSUTF8StringEncoding]};
    return bytes;
}
KCBytes BytesFromNSData  (NSData* aData)
{
    KCBytes bytes = (KCBytes) {.data = (const char *)[aData bytes], .length = [aData length]};
    return bytes;
}

KCDBOptions MakeDBOptions(bool aCreateIfMissing,
                          bool aCreateIntermediateDirectories,
                          bool aErrorIfExists,
                          bool aParanoidCheck,
                          bool aCompression,
                          int aFilterPolicy,
                          size_t aCacheSize,
                          size_t aBlockSize,
                          size_t aWriteBufferSize)
{
    return (KCDBOptions) {aCreateIfMissing, aCreateIntermediateDirectories, aErrorIfExists, aParanoidCheck, aCompression, aFilterPolicy, aCacheSize, aBlockSize, aWriteBufferSize};
}

KCDBOptions MakeDefaultDBOptions()
{
    return MakeDBOptions(true, true, false, false, true, 0, 0,0,0);
}

@interface KCIterator ()
- (id)initWithDBIter:(leveldb::Iterator*)aIter;
@end

@interface KCWriteBatch ()
+ (instancetype) createWriteBatchFromDB:(id)db;
- (leveldb::WriteBatch) writeBatch;
@end

@interface KCSnapshot ()
+ (id)createSnapshotFromDB:(KCDB *)aDB;
- (const leveldb::Snapshot *)getSnapshot;
@end

@interface KCDB ()
{
    leveldb::DB * m_db;
    leveldb::ReadOptions m_readOptions;
    leveldb::WriteOptions m_writeOptions;
    const leveldb::Cache * m_cache;
    const leveldb::FilterPolicy * m_filterPolicy;
    
    NSString* m_path;
    KCDBOptions m_dbOptions;
}

@property (nonatomic, readonly) leveldb::DB * db;

@end

@implementation KCDB

@synthesize db   = m_db;
@synthesize path = m_path;

#pragma mark - DB MANAGEMENT
- (BOOL)open
{
    leveldb::Options options;
    options.create_if_missing = m_dbOptions.createIfMissing;
    options.paranoid_checks = m_dbOptions.paranoidCheck;
    options.error_if_exists = m_dbOptions.errorIfExists;
    
    if (!m_dbOptions.compression)
        options.compression = leveldb::kNoCompression;
    else
        options.compression = leveldb::kSnappyCompression;
    
    if (m_dbOptions.cacheSize > 0)
    {
        options.block_cache = leveldb::NewLRUCache(m_dbOptions.cacheSize);
        m_cache = options.block_cache;
    }
    else
        m_readOptions.fill_cache = false;
    
    if (m_dbOptions.createIntermediateDirectories)
    {
        NSString *dirpath = [m_path stringByDeletingLastPathComponent];
        NSFileManager *fm = [NSFileManager defaultManager];
        
        
        BOOL isDir = YES;
        if (![fm fileExistsAtPath:dirpath isDirectory:&isDir])
        {
            NSError *crError;
            BOOL success = [fm createDirectoryAtPath:dirpath
                         withIntermediateDirectories:true
                                          attributes:nil
                                               error:&crError];
            if (!success)
            {
                NSLog(@"Problem creating parent directory: %@", crError);
                return false;
            }
        }

    }
    
    if (m_dbOptions.blockSize > 0)
    {
        options.block_size = m_dbOptions.blockSize;
    }
    if (m_dbOptions.writeBufferSize > 0)
    {
        options.write_buffer_size = m_dbOptions.writeBufferSize;
    }
    
    if (m_dbOptions.filterPolicy > 0)
    {
        m_filterPolicy = leveldb::NewBloomFilterPolicy(m_dbOptions.filterPolicy);;
        options.filter_policy = m_filterPolicy;
    }
    leveldb::Status status = leveldb::DB::Open(options, [m_path UTF8String], &m_db);
    
    if(!status.ok())
    {
        NSLog(@"Problem creating LevelDB database: %s", status.ToString().c_str());
        return false;
    }
    
    return true;
}
- (BOOL)isOpen
{
    return !(m_db == NULL);
}
- (void)close
{
    @synchronized(self)
    {
        if (m_db)
        {
            delete m_db;
            if (m_cache)
                delete m_cache;
            
            if (m_filterPolicy)
                delete m_filterPolicy;
            
            m_db = NULL;
        }
    }
}

- (void)destroy
{
    leveldb::Options options;
    options.create_if_missing = true;
    const char* slicePath = [m_path UTF8String];
    leveldb::Status status = DestroyDB(slicePath, options);
    if (!status.ok())
    {
        NSLog(@"Problem destroy in database: %s", status.ToString().c_str());
    }
}


- (KCSnapshot *)createSnapshot
{
    KCSnapshot* snapshot = [KCSnapshot createSnapshotFromDB:self];
    return snapshot;
}

#pragma mark - CREATE

- (void)putBytes:(KCBytes&)aValue keyBytes:(KCBytes&)aKey sync:(BOOL)aSync
{
    if (aValue.data && aKey.data)
    {
        [self putSlice:SliceFromBytes(aValue) keySlice:SliceFromBytes(aKey) sync:m_writeOptions.sync];
    }
}

- (void)put:(NSData*)aValue keyData:(NSData*)aKey
{
    if (aValue && aKey)
    {
        [self putSlice:SliceFromNSData(aValue) keySlice:SliceFromNSData(aKey) sync:m_writeOptions.sync];
    }
}

- (void)put:(NSData*)aValue key:(NSString*)aKey
{
    if (aValue && aKey)
    {
        [self putSlice:SliceFromNSData(aValue) keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }
}

- (void)putString:(NSString*)aValue key:(NSString*)aKey
{
    if (aValue && aKey)
    {
        [self putSlice:SliceFromNSString(aValue) keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }
}


- (void)putDBObject:(id<KCDBObject>)aValue key:(NSString*)aKey
{
    if (aValue && aKey)
    {
        NSData* data = [aValue toBytes];
        if (data && data.length>0)
        {
            [self putSlice:SliceFromNSData(data) keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
        }
    }
}


- (void)putInt:(int)aValue key:(NSString*)aKey
{
    if (aKey)
    {
        leveldb::Slice value((char*) &aValue, sizeof(int));
        [self putSlice:value keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }
    
}
- (void)putShort:(short)aValue key:(NSString*)aKey
{
    if (aKey)
    {
        leveldb::Slice value((char*) &aValue, sizeof(short));
        [self putSlice:value keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }
}

- (void)putBoolean:(bool)aValue key:(NSString*)aKey
{
    if (aKey)
    {
        leveldb::Slice value((char*) &aValue, sizeof(bool));
        [self putSlice:value keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }
}


- (void)putDouble:(double)aValue key:(NSString*)aKey
{
    if (aKey)
    {
        std::ostringstream oss;
        oss << std::setprecision(17) << aValue;
        std::string value = oss.str();
        [self putSlice:value keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }
}

- (void)putFloat:(float)aValue key:(NSString*)aKey
{
    if (aKey)
    {
        std::ostringstream oss;
        oss << std::setprecision(16) << aValue;
        std::string value = oss.str();
        [self putSlice:value keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }

}

- (void)putLong:(long)aValue key:(NSString*)aKey
{
    if (aKey)
    {
        leveldb::Slice value((char*) &aValue, sizeof(long));
        [self putSlice:value keySlice:SliceFromNSString(aKey) sync:m_writeOptions.sync];
    }
}

#pragma mark - DELETE

- (void)removeWithKeyData:(NSData*)aKey
{
    [self removeWithKeyData:aKey sync:m_writeOptions.sync];
}

- (void)removeWithKeyData:(NSData*)aKey sync:(BOOL)aSync
{
    AssertDB(m_db);
    
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = aSync;
    
    leveldb::Slice k = SliceFromNSData(aKey);
    leveldb::Status status = m_db->Delete(writeOptions, k);
    
    if(!status.ok())
    {
        NSLog(@"Problem deleting key/value pair in database: %s", status.ToString().c_str());
    }
}


- (void)remove:(NSString*)aKey
{
    [self remove:aKey sync:m_writeOptions.sync];
}

- (void)remove:(NSString*)aKey sync:(BOOL)aSync
{
    AssertDB(m_db);
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = aSync;
    leveldb::Slice k = SliceFromNSString(aKey);
    leveldb::Status status = m_db->Delete(writeOptions, k);
    
    if(!status.ok())
    {
        NSLog(@"Problem deleting key/value pair in database: %s", status.ToString().c_str());
    }
}


- (void)removeAllWithPrefix:(id)aPrefix
{
    AssertDB(m_db);
    
    leveldb::Iterator * iter = m_db->NewIterator(m_readOptions);
    leveldb::Slice lkey;
    
    const void *prefixPtr;
    size_t prefixLen = 0;
    aPrefix = [self toNSData:aPrefix];
    if (aPrefix)
    {
        prefixPtr = [(NSData *)aPrefix bytes];
        prefixLen = (size_t)[(NSData *)aPrefix length];
    }
    
    for (SeekToKey(iter, (id)aPrefix, NO)
         ; iter->Valid()
         ; MoveCursor(iter, NO))
    {
        
        lkey = iter->key();
        if (aPrefix && memcmp(lkey.data(), prefixPtr, MIN(prefixLen, lkey.size())) != 0)
            break;
        
        m_db->Delete(m_writeOptions, lkey);
    }
    delete iter;
}


#pragma mark - WRITE

- (KCWriteBatch *)createWritebatch
{
    KCWriteBatch* writebatch = [KCWriteBatch createWriteBatchFromDB:self];
    KCRetain(writebatch);
    return writebatch;
}

- (void)write:(KCWriteBatch*)aWriteBatch
{
    [self write:aWriteBatch sync:m_writeOptions.sync];
}

- (void)write:(KCWriteBatch*)aWriteBatch sync:(BOOL)aSync
{
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = aSync;
    
    leveldb::WriteBatch wb = [aWriteBatch writeBatch];
    leveldb::Status status = m_db->Write(writeOptions, &wb);
    if(!status.ok())
    {
        NSLog(@"Problem applying the write batch in database: %s", status.ToString().c_str());
    }
}

#pragma mark - RETRIEVE
- (NSData*)getWithKeyData:(NSData*)aKey snapshot:(KCSnapshot*)aSnapshot
{
    if (aKey && aKey.length > 0)
    {
        KCBytesStatus value = [self getWithKeySlice:SliceFromNSData(aKey) snapshot:aSnapshot];
        if (value.status.ok())
        {
            NSData* dataValue = NSDataFromBytes(value.bytes);
            return dataValue;
        }
    }
    
    return nil;
}

- (NSData*)get:(NSString*)aKey snapshot:(KCSnapshot*)aSnapshot
{
    if (aKey && aKey.length > 0)
    {
        KCBytesStatus value = [self getWithKeySlice:SliceFromNSString(aKey) snapshot:aSnapshot];
        if (value.status.ok())
        {
            NSData* dataValue = NSDataFromBytes(value.bytes);
            return dataValue;
        }
    }
    return nil;
}

- (NSData*)getWithKeyData:(NSData*)aKey
{
    return [self getWithKeyData:aKey snapshot:nil];
}

- (NSData*)get:(NSString*)aKey
{
    return [self get:aKey snapshot:nil];
}

- (NSString*)getString:(NSString*)aKey
{
    if (aKey && aKey.length > 0)
    {
        KCBytesStatus value = [self getWithKeySlice:SliceFromNSString(aKey) snapshot:nil];
        if (value.status.ok())
        {
            return NSStringFromBytes(value.bytes);
        }
    }
    return nil;
}

- (short)getShort:(NSString*)aKey
{
    std::string data;
    leveldb::Status status = m_db->Get(m_readOptions, SliceFromNSString(aKey), &data);
    
    if (status.ok())
    {
        if (sizeof(short) <= data.length())
        {
            const char* bytes = data.data();
            short ret = 0;
            ret = (unsigned char)bytes[1];
            ret = (ret << 8) + (unsigned char)bytes[0];
            return ret;
        }
        else
        {
            NSLog(@"getShort error, the data is not short, the data length is %lu", data.length());
        }
    }
    else
    {
        std::string err("Failed to get a short: " + status.ToString());
        NSLog(@"%@", NSStringFromSlice(err));
    }
    
    return 0;
}


- (int)getInt:(NSString*)aKey
{
    std::string data;
    leveldb::Status status = m_db->Get(m_readOptions, SliceFromNSString(aKey), &data);
    
    if (status.ok())
    {
        if (sizeof(int) <= data.length())
        {
            const char* bytes = data.data();
            int ret = 0;
            ret = (unsigned char)bytes[3];
            ret = (ret << 8) + (unsigned char)bytes[2];
            ret = (ret << 8) + (unsigned char)bytes[1];
            ret = (ret << 8) + (unsigned char)bytes[0];
            
            return ret;
        }
        else
        {
            NSLog(@"getInt error, the data is not int, the data length is %lu", data.length());
        }
    }
    else
    {
        std::string err("Failed to get an int: " + status.ToString());
        NSLog(@"%@", NSStringFromSlice(err));
    }
    
    return 0;
}

- (BOOL)getBoolean:(NSString*)aKey
{
    std::string data;
    leveldb::Status status = m_db->Get(m_readOptions, SliceFromNSString(aKey), &data);
    
    if (status.ok())
    {
        if (sizeof(bool) <= data.length())
        {
//            if (data == "0")
//                return NO;
            return data.data()[0];
        }
        else
        {
            NSLog(@"getBoolean error, the data is not int, the data length is %lu", data.length());
        }
    }
    else
    {
        std::string err("Failed to get boolean: " + status.ToString());
        NSLog(@"%@", NSStringFromSlice(err));
    }
    
    return false;
}

- (double)getDouble:(NSString*)aKey
{
    std::string data;
    leveldb::Status status = m_db->Get(m_readOptions, SliceFromNSString(aKey), &data);
    
    if (status.ok())
    {
        // we can't use data.length() here to make sure of the size of float since it was encoded as string
        double d = atof(data.c_str());
        return d;
    }
    else
    {
        std::string err("Failed to get a double: " + status.ToString());
        NSLog(@"%@", NSStringFromSlice(err));
    }
    
    return 0;
}

- (float)getFloat:(NSString*)aKey
{
    std::string data;
    leveldb::Status status = m_db->Get(m_readOptions, SliceFromNSString(aKey), &data);
    
    if (status.ok())
    {
        // we can't use data.length() here to make sure of the size of float since it was encoded as string
        float f = atof(data.c_str());
        return f;
    }
    else
    {
        std::string err("Failed to get a float: " + status.ToString());
        NSLog(@"%@", NSStringFromSlice(err));
    }
    
    return 0;
}

- (long)getLong:(NSString*)aKey
{
    std::string data;
    leveldb::Status status = m_db->Get(m_readOptions, SliceFromNSString(aKey), &data);
    
    if (status.ok())
    {
        if (sizeof(long) <= data.length())
        {
            const char* bytes = data.data();
            long long ret = 0;
            
            ret = bytes[7];
            ret = (ret << 8) + (unsigned char)bytes[6];
            ret = (ret << 8) + (unsigned char)bytes[5];
            ret = (ret << 8) + (unsigned char)bytes[4];
            ret = (ret << 8) + (unsigned char)bytes[3];
            ret = (ret << 8) + (unsigned char)bytes[2];
            ret = (ret << 8) + (unsigned char)bytes[1];
            ret = (ret << 8) + (unsigned char)bytes[0];
            return ret;
        }
        else
        {
            NSLog(@"getLong error, the data is not long, the data length is %lu", data.length());
        }
    }
    else
    {
        std::string err("Failed to get an long: " + status.ToString());
        NSLog(@"%@", NSStringFromSlice(err));
    }
    
    return 0;
}

#pragma mark - KEYS OPERATIONS
- (BOOL)exists:(NSString*)aKey
{
    return [self existsWithKey:aKey snapshot:nil];
}

- (BOOL)exists:(NSString*)aKey snapshot:(KCSnapshot*)aSnapshot
{
    return [self existsWithKey:aKey snapshot:aSnapshot];
}
- (BOOL)existsKeyData:(NSData*)aKey snapshot:(KCSnapshot*)aSnapshot
{
    return [self existsWithKey:aKey snapshot:aSnapshot];
}

- (NSArray*)findKeys:(NSString*)aPrefix
{
    return [self findKeys:aPrefix offset:0 limit:LIMIT_MAX];
}
- (NSArray*)findKeys:(NSString*)aPrefix offset:(int)aOffset
{
    return [self findKeys:aPrefix offset:aOffset limit:LIMIT_MAX];;
}
- (NSArray*)findKeys:(NSString*)aPrefix offset:(int)aOffset limit:(int)aLimit
{
    AssertDB(m_db);
    
    NSMutableArray* list = [[NSMutableArray alloc]init];
    KCAutorelease(list);
    
    if (aPrefix && aOffset>=0 && aLimit>=0 )
    {
        leveldb::Slice prefix = SliceFromNSString(aPrefix);
        std::vector<std::string> result;
        leveldb::Iterator* it = m_db->NewIterator(m_readOptions);
        
        int count = 0;
        for (it->Seek(prefix); count < (aOffset + aLimit) && it->Valid() && it->key().starts_with(prefix);
             it->Next())
        {
            if (count >= aOffset)
            {
                leveldb::Slice sliceKey = it->key();
                result.push_back(sliceKey.ToString());
                
                [list addObject:NSStringFromSlice(sliceKey)];
            }
            ++count;
        }
        
        delete it;
    }
    
    return list;
}

- (int)countKeys:(NSString*)aPrefix
{
    if (!aPrefix) return 0;
    AssertDB(m_db);
    
    leveldb::Slice prefix = SliceFromNSString(aPrefix);
    leveldb::Iterator* it = m_db->NewIterator(leveldb::ReadOptions());
    
    int count = 0;
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next())
    {
        ++count;
    }
    delete it;
    
    return count;
}

- (NSArray*)findKeysBetween:(NSString*)aStartPrefix endPrefix:(NSString*)aEndPrefix
{
    return [self findKeysBetween:aStartPrefix endPrefix:aEndPrefix offset:0 limit:LIMIT_MAX];
}
- (NSArray*)findKeysBetween:(NSString*)aStartPrefix endPrefix:(NSString*)aEndPrefix offset:(int)aOffset
{
    return [self findKeysBetween:aStartPrefix endPrefix:aEndPrefix offset:aOffset limit:LIMIT_MAX];
}
- (NSArray*)findKeysBetween:(NSString*)aStartPrefix endPrefix:(NSString*)aEndPrefix offset:(int)aOffset limit:(int)aLimit
{
    AssertDB(m_db);
    
    NSMutableArray* list = [[NSMutableArray alloc]init];
    KCAutorelease(list);
    
    if (aStartPrefix && aEndPrefix && aOffset>=0 && aLimit>=0 )
    {
        leveldb::Slice startPrefix = SliceFromNSString(aStartPrefix);
        leveldb::Slice endPrefix = SliceFromNSString(aEndPrefix);
        
        std::vector<std::string> result;
        leveldb::Iterator* it = m_db->NewIterator(leveldb::ReadOptions());
        
        int count = 0;
        for (it->Seek(startPrefix); count < (aOffset + aLimit) && it->Valid() && it->key().compare(endPrefix) <= 0;
             it->Next())
        {
            if (count >= aOffset)
            {
                leveldb::Slice sliceKey = it->key();
                result.push_back(sliceKey.ToString());
                
                [list addObject:NSStringFromSlice(sliceKey)];
            }
            ++count;
        }
        
        delete it;
    }
    
    return list;
}

- (int)countKeysBetween:(NSString*)aStartPrefix endPrefix:(NSString*)aEndPrefix
{
    AssertDB(m_db);
    if (!aStartPrefix && !aEndPrefix) return 0;
    
    leveldb::Slice startPrefix = SliceFromNSString(aStartPrefix);
    leveldb::Slice endPrefix = SliceFromNSString(aEndPrefix);
    
    leveldb::Iterator* it = m_db->NewIterator(leveldb::ReadOptions());
    
    int count = 0;
    for (it->Seek(startPrefix); it->Valid() && it->key().compare(endPrefix) <= 0; it->Next())
    {
        ++count;
    }
    delete it;
    
    return count;
}

#pragma mark - ITERATORS
- (KCIterator*)iterator
{
    return [self iteratorWithSnapshot:nil];
}
- (KCIterator*)iteratorWithSnapshot:(KCSnapshot*)aSnapshot
{
    AssertDB(m_db);
    MaybeAddSnapshotToOptions(m_readOptions, readOptionsPtr, aSnapshot);
    leveldb::Iterator* pIter = m_db->NewIterator(*readOptionsPtr);
    KCIterator* iter = [[KCIterator alloc] initWithDBIter:pIter];
    KCAutorelease(iter);
    return iter;
}

- (KCIterator*)iteratorWithSnapshot:(KCSnapshot*)aSnapshot fillCache:(BOOL)aFillCache
{
    AssertDB(m_db);
    leveldb::ReadOptions options = leveldb::ReadOptions();
    if (aSnapshot)
        options.snapshot = aSnapshot.getSnapshot;
    options.fill_cache = aFillCache;
    leveldb::Iterator* pIter = m_db->NewIterator(options);
    KCIterator* iter = [[KCIterator alloc] initWithDBIter:pIter];
    KCAutorelease(iter);
    return iter;
}

- (KCBytes)getPropertyBytes:(KCBytes&)aKey
{
    leveldb::Slice keySlice = SliceFromBytes(aKey);
    std::string value;
    bool ok = m_db->GetProperty(keySlice, &value);
    KCBytes emptyBytes;
    if (ok)
    {
        if (value.length() < 1)
        {
            return emptyBytes;
        }
        
        return BytesFromSlice(value);
    }
    return emptyBytes;
}

- (BOOL)repairDB
{
    leveldb::Status status = leveldb::RepairDB([m_path UTF8String], leveldb::Options());
    
    if(!status.ok())
    {
        NSLog(@"Problem creating LevelDB database: %s", status.ToString().c_str());
        return false;
    }
    return true;
}


#pragma mark - KCDB Object

- (void)setSync:(BOOL)safe
{
    m_writeOptions.sync = safe;
}
- (BOOL)sync
{
    return m_writeOptions.sync;
}
- (void)setFillCache:(BOOL)fillCache
{
    m_readOptions.fill_cache = fillCache;
}
- (BOOL)fillCache
{
    return m_readOptions.fill_cache;
}

+ (KCDBOptions)makeDefaultOptions
{
    return MakeDefaultDBOptions();
}

- (id)initWithPath:(NSString *)aPath
{
    KCDBOptions opts = MakeDefaultDBOptions();
    return [self initWithPath:aPath options:opts];
}

- (id)initWithPath:(NSString *)aPath options:(KCDBOptions)aOptions
{
    self = [super init];
    if (self)
    {
        m_path = aPath;
        m_dbOptions = aOptions;
        
        m_readOptions.fill_cache = true;
        m_writeOptions.sync = false;
        
    }
    
    return self;
}


#pragma mark - Write batches

- (void)performWritebatch:(void (^)(KCWriteBatch *wb))block
{
    KCWriteBatch *wb = [self createWritebatch];
    block(wb);
    [wb write];
}



#pragma mark - Enumeration
- (void) _startIterator:(leveldb::Iterator*)aIter backward:(BOOL)aBackward prefix:(id)aPrefix start:(id)aKey
{
    
    const void *prefixPtr;
    size_t prefixLen;
    leveldb::Slice lkey, startingKey;
    
    aPrefix = [self toNSData:aPrefix];
    if (aPrefix)
    {
        prefixPtr = [(NSData *)aPrefix bytes];
        prefixLen = (size_t)[(NSData *)aPrefix length];
        startingKey = leveldb::Slice((char *)prefixPtr, prefixLen);
        
        if (aKey)
        {
            leveldb::Slice skey = SliceFromNSStringOrNSData(aKey);
            if (skey.size() > prefixLen && memcmp(skey.data(), prefixPtr, prefixLen) == 0)
            {
                startingKey = skey;
            }
        }
        
        /*
         * If a prefix is provided and the iteration is backwards
         * we need to start on the next key (maybe discarding the first iteration)
         */
        if (aBackward)
        {
            signed long long i = startingKey.size() - 1;
            void * startingKeyPtr = malloc(startingKey.size());
            unsigned char *keyChar;
            memcpy(startingKeyPtr, startingKey.data(), startingKey.size());
            while (1) {
                if (i < 0)
                {
                    aIter->SeekToLast();
                    break;
                }
                keyChar = (unsigned char *)startingKeyPtr + i;
                if (*keyChar < 255)
                {
                    *keyChar = *keyChar + 1;
                    aIter->Seek(leveldb::Slice((char *)startingKeyPtr, startingKey.size()));
                    if (!aIter->Valid())
                    {
                        aIter->SeekToLast();
                    }
                    break;
                }
                i--;
            };
            free(startingKeyPtr);
            if (!aIter->Valid())
                return;
            
            lkey = aIter->key();
            if (startingKey.size() && aPrefix)
            {
                signed int cmp = memcmp(lkey.data(), startingKey.data(), startingKey.size());
                if (cmp > 0)
                {
                    aIter->Prev();
                }
            }
        }
        else
        {
            // Otherwise, we start at the provided prefix
            aIter->Seek(startingKey);
        }
    }
    else if (aKey)
    {
        aIter->Seek(SliceFromNSStringOrNSData(aKey));
    }
    else if (aBackward)
    {
        aIter->SeekToLast();
    }
    else
    {
        aIter->SeekToFirst();
    }
}



- (void) enumerateKeysBackward:(BOOL)aBackward
                 startingAtKey:(id)aKey
           filteredByPredicate:(NSPredicate *)aPredicate
                     andPrefix:(id)aPrefix
                  withSnapshot:(KCSnapshot *)aSnapshot
                    usingBlock:(KCDBKeyBlock)aBlock
{
    AssertDB(m_db);
    MaybeAddSnapshotToOptions(m_readOptions, readOptionsPtr, aSnapshot);
    leveldb::Iterator* iter = m_db->NewIterator(*readOptionsPtr);
    leveldb::Slice lkey;
    BOOL stop = false;
    
    NSData *prefixData = [self toNSData:aPrefix];
    
    KCDBKeyValueBlock iterate = (aPredicate != nil)
    ? ^(KCBytes *lk, id value, BOOL *stop)
        {
            if ([aPredicate evaluateWithObject:value])
                aBlock(lk, stop);
        }
    : ^(KCBytes *lk, id value, BOOL *stop)
        {
            aBlock(lk, stop);
        };
    
    for ([self _startIterator:iter backward:aBackward prefix:aPrefix start:aKey]
         ; iter->Valid()
         ; MoveCursor(iter, aBackward)) {
        
        lkey = iter->key();
        if (aPrefix && memcmp(lkey.data(), [prefixData bytes], MIN((size_t)[prefixData length], lkey.size())) != 0)
            break;
        
        KCBytes lk = BytesFromSlice(lkey);
        id v = (aPredicate == nil) ? nil :  NSDataFromSlice(iter->value());// can Decode here
        iterate(&lk, v, &stop);
        if (stop) break;
    }
    
    delete iter;
}


- (void) enumerateKeysAndObjectsBackward:(BOOL)aBackward
                                  lazily:(BOOL)aLazily
                           startingAtKey:(id)aKey
                     filteredByPredicate:(NSPredicate *)aPredicate
                               andPrefix:(id)aPrefix
                            withSnapshot:(KCSnapshot *)aSnapshot
                              usingBlock:(id)aBlock
{
    
    AssertDB(m_db);
    MaybeAddSnapshotToOptions(m_readOptions, readOptionsPtr, aSnapshot);
    leveldb::Iterator* iter = m_db->NewIterator(*readOptionsPtr);
    leveldb::Slice lkey;
    BOOL stop = false;
    
    KCDBLazyKeyValueBlock iterate = (aPredicate != nil)
    
    // If there is a predicate:
    ? ^ (KCBytes *lk, KCDBValueGetterBlock valueGetter, BOOL *stop){
        // We need to get the value, whether the `lazily` flag was set or not
        id value = valueGetter();
        
        // If the predicate yields positive, we call the block
        if ([aPredicate evaluateWithObject:value])
        {
            if (aLazily)
                ((KCDBLazyKeyValueBlock)aBlock)(lk, valueGetter, stop);
            else
                ((KCDBKeyValueBlock)aBlock)(lk, value, stop);
        }
    }
    
    // Otherwise, we call the block
    : ^ (KCBytes *lk, KCDBValueGetterBlock valueGetter, BOOL *stop) {
        if (aLazily)
            ((KCDBLazyKeyValueBlock)aBlock)(lk, valueGetter, stop);
        else
            ((KCDBKeyValueBlock)aBlock)(lk, valueGetter(), stop);
    };
    
    NSData *prefixData = [self toNSData:aPrefix];
    
    KCDBValueGetterBlock getter;
    for ([self _startIterator:iter backward:aBackward prefix:aPrefix start:aKey]
         ; iter->Valid()
         ; MoveCursor(iter, aBackward))
    {
        
        lkey = iter->key();
        // If there is prefix provided, and the prefix and key don't match, we break out of iteration
        if (aPrefix && memcmp(lkey.data(), [prefixData bytes], MIN((size_t)[prefixData length], lkey.size())) != 0)
            break;
        
        __block KCBytes lk = BytesFromSlice(lkey);
        __block id v = nil;
        
        getter = ^ id {
            if (v) return v;
            v = NSDataFromSlice(iter->value());// can Decode here
            return v;
        };
        
        iterate(&lk, getter, &stop);
        if (stop) break;
    }
    
    delete iter;
}


#pragma mark - private

- (void) dealloc
{
    [self close];
    KCDealloc(super);
}

- (NSData*)toNSData:(id)aObj
{
    return ([aObj isKindOfClass:[NSData class]]) ? aObj :
    ([aObj isKindOfClass:[NSString class]]) ? [NSData dataWithBytes:[aObj cStringUsingEncoding:NSUTF8StringEncoding]
                                                              length:[aObj lengthOfBytesUsingEncoding:NSUTF8StringEncoding]] : nil;
}



- (BOOL)existsWithKey:(id)aKey snapshot:(KCSnapshot *)aSnapshot
{
    AssertDB(m_db);
    std::string v_string;
    MaybeAddSnapshotToOptions(m_readOptions, readOptionsPtr, aSnapshot);
    leveldb::Slice k = SliceFromNSStringOrNSData(aKey);
    leveldb::Status status = m_db->Get(*readOptionsPtr, k, &v_string);
    
    if (!status.ok())
    {
        if (status.IsNotFound())
            return false;
        else
        {
            NSLog(@"Problem retrieving value for key '%@' from database: %s", aKey, status.ToString().c_str());
            return NULL;
        }
    } else
        return true;
    
    return true;
}


- (BOOL)putSlice:(leveldb::Slice)aValue keySlice:(leveldb::Slice)aKey sync:(BOOL)aSync;
{
//    NSParameterAssert(aValue != nil);
    AssertDB(m_db);
    
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = aSync;
    
    leveldb::Status status = m_db->Put(writeOptions, aKey, aValue);
    if(!status.ok())
    {
        NSLog(@"Problem storing key/value pair in database: %s", status.ToString().c_str());
        return false;
    }
    return true;
}

- (KCBytesStatus)getWithKeySlice:(leveldb::Slice)aKey snapshot:(KCSnapshot *)aSnapshot
{
    AssertDB(m_db);
//    std::string v_string;
    MaybeAddSnapshotToOptions(m_readOptions, readOptionsPtr, aSnapshot);
    
    leveldb::Status status;
    KCBytesStatus bytesStatus;
    leveldb::Iterator* iter = m_db->NewIterator(*readOptionsPtr);
    iter->Seek(aKey);
    if (iter->Valid() && aKey == iter->key())
    {
        leveldb::Slice value = iter->value();
        bytesStatus.bytes = BytesFromSlice(value);
    }
    else
    {
        status = leveldb::Status::NotFound("not found");
    }
    delete iter;
    
    bytesStatus.status = status;
    
//    leveldb::Status status = m_db->Get(*readOptionsPtr, aKey, &v_string);
//    KCBytes bytes = BytesFromSlice(leveldb::Slice(v_string));
//    KCBytesStatus sliceStatus = (KCBytesStatus){.bytes=bytes, .status = status};
    
    if(!status.ok())
    {
        if(!status.IsNotFound())
            NSLog(@"Problem retrieving value for key '%@' from database: %s", NSDataFromSlice(aKey), status.ToString().c_str());
        return bytesStatus;
    }
    return bytesStatus;
    
}



@end



NSString * getLibraryPath()
{
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES);
    return [paths objectAtIndex:0];
}
