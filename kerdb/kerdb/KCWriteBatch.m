//
//  KCWriteBatch.m
//  kerdb
//
//  Created by zihong on 16/5/20.
//  Copyright © 2016年 com.kercer. All rights reserved.
//

#import "KCWriteBatch.h"

#import <leveldb/db.h>
#import <leveldb/write_batch.h>
#include "KCCommon.h"



@interface KCWriteBatch()
{
    leveldb::WriteBatch m_writeBatch;
    id __unsafe_unretained m_db;
    
    dispatch_queue_t m_serial_queue;
}
@property (readonly) leveldb::WriteBatch writeBatch;

@end


@implementation KCWriteBatch

@synthesize writeBatch = m_writeBatch;
@synthesize db = m_db;


+ (instancetype)createWriteBatchFromDB:(id)db
{
    id wb = [[self alloc] init];
    KCAutorelease(wb);
    id dbTmp = db;
    KCRetain(dbTmp);
    ((KCWriteBatch *)wb)->m_db = dbTmp;
    return wb;
}

- (instancetype)init
{
    self = [super init];
    if (self)
    {
        m_serial_queue = dispatch_queue_create(NULL, DISPATCH_QUEUE_SERIAL);
    }
    return self;
}
- (void)dealloc
{
    if (m_serial_queue)
    {
#if !OS_OBJECT_USE_OBJC
        dispatch_release(_serial_queue);
#endif
        m_serial_queue = nil;
    }
    if (m_db)
    {
        KCRelease(m_db);
        m_db = nil;
    }
    
    KCDealloc(super);
}

- (void)removeWithData:(NSData*)aKey;
{
    leveldb::Slice k = SliceFromNSData(aKey);
    dispatch_sync(m_serial_queue, ^{
        m_writeBatch.Delete(k);
    });
}

- (void)remove:(NSString*)aKey
{
    leveldb::Slice k = SliceFromNSString(aKey);
    dispatch_sync(m_serial_queue, ^{
        m_writeBatch.Delete(k);
    });
}


- (void)put:(KCBytes)aValue key:(KCBytes)aKey;
{
    dispatch_sync(m_serial_queue, ^{
        leveldb::Slice k = SliceFromBytes(aKey);
        leveldb::Slice v = SliceFromBytes(aValue);
        
        m_writeBatch.Put(k, v);
    });
}


- (void)write
{
    AssertDB(m_db);
    [m_db write:self];
    [self clear];
    
}

- (void)clear
{
    m_writeBatch.Clear();
}

@end
