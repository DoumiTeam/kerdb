#include <jni.h>
#include <android/log.h>

#include "kerdbjni.h"

#include "leveldb/iterator.h"
#include <vector>

static void nativeClose(JNIEnv* env, jclass clazz, jlong ptr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(ptr);

    delete iter;
}

static void nativeSeekToFirst(JNIEnv* env, jclass clazz, jlong iterPtr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);
    iter->SeekToFirst();
}

static void nativeSeekToLast(JNIEnv* env, jclass clazz, jlong iterPtr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);
    iter->SeekToLast();
}

static void nativeSeek(JNIEnv* env, jclass clazz, jlong iterPtr, jbyteArray keyObj)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);

    size_t keyLen = env->GetArrayLength(keyObj);
    jbyte *buffer = env->GetByteArrayElements(keyObj, NULL);

    iter->Seek(leveldb::Slice((const char *)buffer, keyLen));
    env->ReleaseByteArrayElements(keyObj, buffer, JNI_ABORT);
}

static jboolean nativeValid(JNIEnv* env, jclass clazz, jlong iterPtr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);
    return iter->Valid();
}

static void nativeNext(JNIEnv* env, jclass clazz, jlong iterPtr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);
    iter->Next();
}

static void nativePrev(JNIEnv* env, jclass clazz, jlong iterPtr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);
    iter->Prev();
}

static jbyteArray nativeKey(JNIEnv* env, jclass clazz, jlong iterPtr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);
    leveldb::Slice key = iter->key();

    size_t len = key.size();
    jbyteArray result = env->NewByteArray(len);
    env->SetByteArrayRegion(result, 0, len, (const jbyte *) key.data());
    return result;
}

static jbyteArray nativeValue(JNIEnv* env, jclass clazz, jlong iterPtr)
{
    leveldb::Iterator* iter = reinterpret_cast<leveldb::Iterator*>(iterPtr);
    leveldb::Slice value = iter->value();

    size_t len = value.size();
    jbyteArray result = env->NewByteArray(len);
    env->SetByteArrayRegion(result, 0, len, (const jbyte *) value.data());
    return result;
}


static jobjectArray nativeIteratorNextArray(JNIEnv* env, jclass clazz, jlong ptr, jstring jEndPrefix, jboolean reverse, jint max)
{
	//LOGI("iterator next array");

	std::vector<std::string> result;
	leveldb::Iterator* it = (leveldb::Iterator*) ptr;

	if (!it->Valid())
	{
		throwDBException (env, "iterator is not valid");
		return NULL;
	}

	const char* endPrefix = NULL;
	if (jEndPrefix)
	{
		endPrefix = env->GetStringUTFChars(jEndPrefix, 0);
	}

	int count = 0;
	while (count < max && it->Valid() && (!endPrefix || (!reverse && it->key().compare(endPrefix) <= 0) || (reverse && it->key().compare(endPrefix) >= 0)))
	 {
		result.push_back(it->key().ToString());
    	++count;
        if (reverse) { it->Prev(); }
        else { it->Next(); }
	}

	if (jEndPrefix)
	{
		env->ReleaseStringUTFChars(jEndPrefix, endPrefix);
	}

	std::vector<std::string>::size_type n = result.size();
	jobjectArray ret= (jobjectArray)env->NewObjectArray(n,
		         env->FindClass("java/lang/String"),
		         env->NewStringUTF(""));

	jstring str;
	for (int i=0; i<n ; i++) {
		str = env->NewStringUTF(result[i].c_str());
		env->SetObjectArrayElement(ret, i, str);
		env->DeleteLocalRef(str);
	}

	return ret;
}

static jboolean nativeIteratorIsValid(JNIEnv* env, jclass clazz, jlong ptr, jstring jEndPrefix, jboolean reverse)
{
	//LOGI("iterator is valid");

	leveldb::Iterator* it = (leveldb::Iterator*) ptr;

	if (!it->Valid())
	{
		return false;
	}
	if (jEndPrefix)
	{
		const char* endPrefix = env->GetStringUTFChars(jEndPrefix, 0);
		if ((!reverse && it->key().compare(endPrefix) > 0) || (reverse && it->key().compare(endPrefix) < 0))
		{
			env->ReleaseStringUTFChars(jEndPrefix, endPrefix);
			return false;
		}
		env->ReleaseStringUTFChars(jEndPrefix, endPrefix);
	}
	return true;
}

static JNINativeMethod sMethods[] =
{
        { "nativeClose", "(J)V", (void*) nativeClose },
        { "nativeSeekToFirst", "(J)V", (void*) nativeSeekToFirst },
        { "nativeSeekToLast", "(J)V", (void*) nativeSeekToLast },
        { "nativeSeek", "(J[B)V", (void*) nativeSeek },
        { "nativeValid", "(J)Z", (void*) nativeValid },
        { "nativeNext", "(J)V", (void*) nativeNext },
        { "nativePrev", "(J)V", (void*) nativePrev },
        { "nativeKey", "(J)[B", (void*) nativeKey },
        { "nativeValue", "(J)[B", (void*) nativeValue },
        { "nativeIteratorNextArray", "(JLjava/lang/String;ZI)[Ljava/lang/String;", (void*) nativeIteratorNextArray },
        { "nativeIteratorIsValid", "(JLjava/lang/String;Z)Z", (void*) nativeIteratorIsValid }
};

int register_Iterator(JNIEnv *env) {
    jclass clazz = env->FindClass("com/kercer/kerdb/jnibridge/KCIterator");
    if (!clazz) {
        LOGE("Can't find class com.kercer.kerdb.jnibridge.KCIterator");
        return 0;
    }

    return env->RegisterNatives(clazz, sMethods, NELEM(sMethods));
}
