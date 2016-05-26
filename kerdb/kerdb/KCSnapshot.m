//
//  KCSnapshot.m
//  kerdb
//
//  Created by zihong on 16/5/20.
//  Copyright © 2016年 com.kercer. All rights reserved.
//

#import "KCSnapshot.h"
#import <leveldb/db.h>
#import "KCCommon.h"


@interface KCDB ()

- (leveldb::DB *)db;

@end

@interface KCSnapshot ()
{
    const leveldb::Snapshot * m_snapshot;
}

@property (readonly, getter = getSnapshot) const leveldb::Snapshot * snapshot;
- (const leveldb::Snapshot *) getSnapshot;

@end


@implementation KCSnapshot

+ (KCSnapshot *)createSnapshotFromDB:(KCDB *)database
{
    KCSnapshot *snapshot = [[KCSnapshot alloc] init];
    KCAutorelease(snapshot);
    snapshot->m_snapshot = [database db]->GetSnapshot();
    snapshot->_db = database;
    return snapshot;
}

- (const leveldb::Snapshot *)getSnapshot
{
    return m_snapshot;
}


- (void)close
{
    @synchronized (self)
    {
        if (m_snapshot && _db && [_db isOpen])
        {
            [_db db]->ReleaseSnapshot(m_snapshot);
            m_snapshot = nil;
        }
    }

}

- (void) dealloc
{
    [self close];
    KCDealloc(super);
}


@end
