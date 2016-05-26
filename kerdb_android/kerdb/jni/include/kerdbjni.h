#ifndef KERDBJNI_H_
#define KERDBJNI_H_

#include <jni.h>
#include <android/log.h>
#include "leveldb/status.h"
#include "debug.h"

# define NELEM(x) ((int) (sizeof(x) / sizeof((x)[0])))

jint throwException(JNIEnv* env, leveldb::Status status);
void throwDBException(JNIEnv *env, const char* msg);

#endif /* LEVELDBJNI_H_ */
