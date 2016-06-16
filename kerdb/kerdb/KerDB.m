//
//  KerDB.m
//  kerdb
//
//  Created by zihong on 16/5/27.
//  Copyright © 2016年 com.kercer. All rights reserved.
//

#import "KerDB.h"
#import "KCCommon.h"


#define kDEFAULT_DBNAME @"kerdb"
@implementation KerDB

#pragma mark open
+ (NSString*)getLibraryPath
{
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES);
    return [paths objectAtIndex:0];
}

+ (KCDB*)openWithPath:(NSString *)aDBPath
{
    if (!aDBPath) return nil;
    KCDB* db = [[KCDB alloc] initWithPath:aDBPath];
    KCAutorelease(db)
    
    return [db open] ? db : nil;
}

+ (KCDB*)openWithPath:(NSString *)aDBPath options:(KCDBOptions)aOptions
{
    if (!aDBPath) return nil;
    KCDB* db = [[KCDB alloc] initWithPath:aDBPath options:aOptions];
    KCAutorelease(db)
    
    return [db open] ? db : nil;
}

+ (KCDB*)openWithDBName:(NSString *)aDBName
{
    if (!aDBName) return nil;
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/%@",[self getLibraryPath], aDBName]];
    KCAutorelease(db)
    
    return [db open] ? db : nil;
}

+ (KCDB*)openWithDBName:(NSString *)aDBName options:(KCDBOptions)aOptions
{
    if (!aDBName) return nil;
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/%@",[self getLibraryPath], aDBName] options:aOptions];
    KCAutorelease(db)
    
    return [db open] ? db : nil;
}


+ (KCDB*)openDefaultDB
{
    return [self openWithDBName:kDEFAULT_DBNAME];
}

+ (KCDB*)openDefaultDBWithOptions:(KCDBOptions)aOptions
{
    return [self openWithDBName:kDEFAULT_DBNAME options:aOptions];
}


#pragma mark -
+ (BOOL)repairDBWithPath:(NSString *)aDBPath
{
    return [KCDB repairDBWithPath:aDBPath];
}

@end
