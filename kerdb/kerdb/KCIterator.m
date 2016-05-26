//
//  KCIterator.m
//  kerdb
//
//  Created by zihong on 16/5/20.
//  Copyright © 2016年 com.kercer. All rights reserved.
//

#import "KCIterator.h"
#import "KCCommon.h"
#import "KCDB.h"

@interface KCIterator ()
{
    leveldb::Iterator* m_iter;
}

@end

@implementation KCIterator

- (id)initWithDBIter:(leveldb::Iterator*)aIter
{
    if (self = [super init])
    {
        m_iter = aIter;
    }
    return self;
}

- (void)seekToFirst
{
    AssertDB(m_iter);
    m_iter->SeekToFirst();
}

- (void)seekToLast
{
    AssertDB(m_iter);
    m_iter->SeekToLast();
}

- (void)seek:(KCBytes)aBytes
{
    AssertDB(m_iter);
    m_iter->Seek(SliceFromBytes(aBytes));
}

- (BOOL)isValid
{
    AssertDB(m_iter);
    return m_iter->Valid();
}

- (void)next
{
    AssertDB(m_iter);
    return m_iter->Next();
}

- (void)prev
{
    AssertDB(m_iter);
    return m_iter->Prev();
}

- (KCBytes)getKey
{
    AssertDB(m_iter);
    leveldb::Slice key = m_iter->key();
    return BytesFromSlice(key);
}

- (KCBytes)getValue
{
    leveldb::Slice value = m_iter->value();
    return BytesFromSlice(value);
}

- (void)close
{
    delete m_iter;
}


@end
