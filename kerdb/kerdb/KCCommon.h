

#pragma once

#if ! __has_feature(objc_arc)
#define KCAutorelease(__v) ([__v autorelease]);
#define KCReturnAutoreleased KCAutorelease

#define KCRetain(__v) ([__v retain]);
#define KCReturnRetained KCRetain

#define KCRelease(__v) ([__v release]);

#define KCDealloc(__v) ([__v dealloc]);
#else
// -fobjc-arc
#define KCAutorelease(__v)
#define KCReturnAutoreleased(__v) (__v)

#define KCRetain(__v)
#define KCReturnRetained(__v) (__v)

#define KCRelease(__v) ([__v class])

#define KCDealloc(__v) ([__v class])
#endif

#define AssertDB(_db_) \
    NSAssert(_db_ != NULL, @"Database reference is not existent (it has probably been closed)");

#define SliceFromNSString(_string_)           leveldb::Slice((char *)[_string_ UTF8String], [_string_ lengthOfBytesUsingEncoding:NSUTF8StringEncoding])
#define NSStringFromSlice(_slice_)            [[NSString alloc] initWithBytes:_slice_.data() length:_slice_.size() encoding:NSUTF8StringEncoding] 

#define SliceFromNSData(_data_)               leveldb::Slice((char *)[_data_ bytes], [_data_ length])
#define NSDataFromSlice(_slice_)              [NSData dataWithBytes:_slice_.data() length:_slice_.size()]
#define SliceFromNSStringOrNSData(_key_)          ([_key_ isKindOfClass:[NSString class]]) ? SliceFromNSString(_key_) \
    : SliceFromNSData(_key_)

#define BytesFromSlice(_slice_)        (KCBytes) { .data = _slice_.data(), .length = _slice_.size() }
#define SliceFromBytes(_bytes_)         leveldb::Slice((char*)_bytes_.data, _bytes_.length)

#define ToNSData(_id_)   ([_id_ isKindOfClass:[NSData class]]) ? _id_ :\
                        ([_id_ isKindOfClass:[NSString class]]) ? [NSData dataWithBytes:[_id_ cStringUsingEncoding:NSUTF8StringEncoding] length:[_id_ lengthOfBytesUsingEncoding:NSUTF8StringEncoding]] : nil
