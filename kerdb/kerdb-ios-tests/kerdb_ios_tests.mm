//
//  kerdb_ios_tests.m
//  kerdb
//
//  Created by zihong on 16/5/20.
//  Copyright © 2016年 com.kercer. All rights reserved.
//

#import <UIKit/UIKit.h>
#import <XCTest/XCTest.h>

#import <kerdb/kerdb.h>

@interface kerdb_ios_tests : XCTestCase

@end

@implementation kerdb_ios_tests

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}


NSString* getLibraryPath()
{
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES);
    return [paths objectAtIndex:0];
}

- (void)testOpenDestoryDB
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    XCTAssertTrue([db open]);
    XCTAssertTrue([db isOpen]);
    [db close];
    [db destroy];
    
}

- (void)testKeyWithData
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        NSData* dataValue = [@"testdata" dataUsingEncoding:NSUTF8StringEncoding];
        NSData* dataKey = [@"keydata" dataUsingEncoding:NSUTF8StringEncoding];
        [db put:dataValue keyData:dataKey];
        
        NSData* dataGetValue = [db getWithKeyData:dataKey snapshot:nil];
        XCTAssertNotNil(dataGetValue);
        NSString *strGetValue = [[NSString alloc] initWithData:dataGetValue  encoding:NSUTF8StringEncoding];
        XCTAssert([strGetValue isEqualToString:@"testdata"]);
        KCRelease(strGetValue);
        
        NSData* dataKey2 = [@"keydata2" dataUsingEncoding:NSUTF8StringEncoding];
        NSData* dataGetValue2 = [db getWithKeyData:dataKey2 snapshot:nil];
        XCTAssertNil(dataGetValue2);
        
        [db close];
        
    }
    
}

- (void)testKeyWithString
{
//    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES);
    NSString* dbPath =[NSString stringWithFormat:@"%@/zihong_db",[paths objectAtIndex:0]];
    
    KCDB* db = [KerDB openWithPath:dbPath];
    if (db)
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
}

- (void)testString
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        NSString* key = @"putString_keyString";
        NSString* value = @"putString";
        [db putString:value key:key];
        NSString* strGetValue = [db getString:key];
        XCTAssert([strGetValue isEqualToString:value]);
        [db close];
        
    }
}

- (void)testShort
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        NSString* key = @"putShort_keyString";
        short valueShort = 88;
        [db putShort:valueShort key:key];
        short shortGetValue = [db getShort:key];
        XCTAssert(shortGetValue == valueShort);
        
        NSString* key2 = @"putShort_keyString2";
        int valueInt = 32768; //short max +1
        [db putShort:valueInt key:key2];
        short shortGetValue2 = [db getShort:key2];
        XCTAssert(shortGetValue2 != valueInt);
        
        [db close];
        
    }
}


- (void)testInt
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        NSString* key = @"putInt_keyString";
        int value = 99;
        [db putInt:value key:key];
        int getValue = [db getInt:key];
        XCTAssert(getValue == value);
        
        NSString* key2 = @"putInt_keyString2";
        long value2 = (long)INT_MAX +1;; //int max +1
        [db putInt:(int)value2 key:key2];
        int getValue2 = [db getInt:key2];
        XCTAssert(getValue2 != value2);
        
        [db close];
        
    }
}

- (void)testBoolean
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        //put bool, get bool
        NSString* key = @"putBoolean_keyString";
        [db putBoolean:YES key:key];
        BOOL getValue = [db getBoolean:key];
        XCTAssert(getValue == YES);
        
        NSString* key2 = @"putBoolean_keyString2";
        [db putBoolean:false key:key2];
        BOOL getValue2 = [db getBoolean:key2];
        XCTAssert(getValue2 == NO);
        
        
        //put int,get bool
        NSString* key3 = @"putBoolean_keyString3";
        [db putInt:1 key:key3];
        BOOL getValue3 = [db getBoolean:key3];
        XCTAssert(getValue3 == true);
        
        NSString* key4 = @"putBoolean_keyString4";
        [db putInt:0 key:key4];
        BOOL getValue4 = [db getBoolean:key4];
        XCTAssert(getValue4 == NO);
        
        NSString* key5 = @"putBoolean_keyString5";
        [db putInt:-2 key:key5];
        BOOL getValue5 = [db getBoolean:key5];
        XCTAssert(getValue5 == YES);
        
        
        //put long,get bool
        NSString* key6 = @"putBoolean_keyString6";
        [db putDouble:1.0 key:key6];
        BOOL getValue6 = [db getBoolean:key6];
        XCTAssert(getValue6 == true);
        
        NSString* key7 = @"putBoolean_keyString7";
        [db putDouble:0.0 key:key7];
        BOOL getValue7 = [db getBoolean:key7];
        XCTAssert(getValue7 == YES);
        
        NSString* key8 = @"putBoolean_keyString8";
        [db putDouble:0.1 key:key8];
        BOOL getValue8 = [db getBoolean:key8];
        XCTAssert(getValue8 == YES);
        
        NSString* key9 = @"putBoolean_keyString9";
        [db putDouble:-2.0 key:key9];
        BOOL getValue9 = [db getBoolean:key9];
        XCTAssert(getValue9 == YES);
        
        
        [db close];
        
    }
}

- (void)testDouble
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        NSString* key = @"putDouble_keyString";
        double value = 99.0;
        [db putDouble:value key:key];
        double getValue = [db getDouble:key];
        XCTAssert(getValue);
        
        NSString* key2 = @"putDouble_keyString2";
        double value2 = 8388609.5; //float max + 1.5
        [db putDouble:value2 key:key2];
        double getValue2 = [db getDouble:key2];
        XCTAssert(getValue2);
        
        NSString* key3 = @"putDouble_keyString2";
        long value3 = 4503599627370497; //double max + 1
        [db putDouble:value3 key:key3];
        double getValue3 = [db getDouble:key3];
        XCTAssert(getValue3);
        
        [db close];
        
    }
}


- (void)testFloat
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        NSString* key = @"putFloat_keyString";
        double value = 199.8;
        [db putFloat:value key:key];
        double getValue = [db getFloat:key];
        XCTAssert(getValue);
        
        NSString* key2 = @"putFloat_keyString2";
        float value2 = 8388609.7; //float max + 1.7
        [db putFloat:value2 key:key2];
        double getValue2 = [db getFloat:key2];
        XCTAssert(getValue2);
        
        NSString* key3 = @"putFloat_keyString2";
        long value3 = 4503599627370497; //double max + 1
        [db putFloat:value3 key:key3];
        double getValue3 = [db getFloat:key3];
        XCTAssert(getValue3);
        
        [db close];
        
    }
}


- (void)testLong
{
    KCDB* db = [[KCDB alloc] initWithPath:[NSString stringWithFormat:@"%@/zihong_db",getLibraryPath()]];
    if ([db open])
    {
        NSString* key = @"putLong_keyString";
        int value = 990;
        [db putLong:value key:key];
        int getValue = [db getInt:key];
        XCTAssert(getValue == value);
        
        NSString* key2 = @"putLong_keyString2";
        long value2 = (long)INT_MAX +1; //int max +1
        [db putLong:value2 key:key2];
        int getValue2 = [db getInt:key2];
        XCTAssert(getValue2 != value2);
        
        
        
        NSString* key3 = @"putLong_keyString3";
        int value3 = 999;
        [db putLong:value3 key:key3];
        long getValue3 = [db getLong:key3];
        XCTAssert(getValue3 == value3);
        
        NSString* key4 = @"putLong_keyString4";
        long value4 = (long)INT_MAX +1;; //int max +1
        [db putLong:value4 key:key4];
        long getValue4 = [db getLong:key4];
        XCTAssert(getValue4 == value4);
        
        [db close];
        
    }
}

@end
