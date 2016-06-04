#include <string.h>
#include <jni.h>

#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <iomanip>
#include <vector>
#include <android/log.h>

#include "kerdbjni.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/env.h"
#include "leveldb/cache.h"
#import "leveldb/filter_policy.h"

static jmethodID gByteBuffer_isDirectMethodID;
static jmethodID gByteBuffer_positionMethodID;
static jmethodID gByteBuffer_limitMethodID;
static jmethodID gByteBuffer_arrayMethodID;


// Redirects leveldb's logging to the Android logger.
class AndroidLogger : public leveldb::Logger
{
public:
  virtual void Logv(const char* format, va_list ap)
  {
    __android_log_vprint(ANDROID_LOG_INFO, "kerdb:N", format, ap);
  }
};

// Holds references to heap-allocated native objects so that they can be
// closed in Java_com_github_hf_leveldb_implementation_NativeLevelDB_nclose.
class KCDBHolder {
public:
  KCDBHolder(leveldb::DB* ldb, AndroidLogger* llogger, leveldb::Cache* lcache, const leveldb::FilterPolicy* lfilterPolicy)
  : db(ldb), logger(llogger), cache(lcache),filterPolicy(lfilterPolicy) {}

  leveldb::DB* db;
  AndroidLogger* logger;

  leveldb::Cache* cache;
  const leveldb::FilterPolicy* filterPolicy;
};

static jlong nativeOpen(JNIEnv* env, jclass clazz, jstring dbpath, jboolean createIfMissing, jint cacheSize, jint blockSize, jint writeBufferSize,
jboolean errorIfExists, jboolean paranoidCheck, jboolean compression, jint filterPolicy)
{
    static bool gInited;

    if (!gInited)
    {
      jclass byteBuffer_Clazz = env->FindClass("java/nio/ByteBuffer");
      gByteBuffer_isDirectMethodID = env->GetMethodID(byteBuffer_Clazz,
                                                      "isDirect", "()Z");
      gByteBuffer_positionMethodID = env->GetMethodID(byteBuffer_Clazz,
                                                      "position", "()I");
      gByteBuffer_limitMethodID = env->GetMethodID(byteBuffer_Clazz,
                                                   "limit", "()I");
      gByteBuffer_arrayMethodID = env->GetMethodID(byteBuffer_Clazz,
                                                   "array", "()[B");
      gInited = true;
    }

    const char *path = env->GetStringUTFChars(dbpath, 0);
    LOGI("Opening database %s", path);

    leveldb::DB* db;

    AndroidLogger* logger = new AndroidLogger();
    leveldb::Cache* cache = NULL;

     if (cacheSize > 0)
     {
        cache = leveldb::NewLRUCache((size_t) cacheSize);
     }

    leveldb::Options options;
    options.create_if_missing = createIfMissing == JNI_TRUE;
    options.paranoid_checks = paranoidCheck == JNI_TRUE;
    options.error_if_exists = errorIfExists == JNI_TRUE;
    if (compression == JNI_TRUE)
    {
      options.compression = leveldb::kSnappyCompression;
    }
    else
    {
        options.compression = leveldb::kNoCompression;
    }

    options.info_log = logger;

    if (cache != NULL)
    {
        options.block_cache = cache;
    }
    if (blockSize > 0)
    {
       options.block_size = (size_t) blockSize;
    }
    if (writeBufferSize > 0)
    {
       options.write_buffer_size = (size_t) writeBufferSize;
    }

    const leveldb::FilterPolicy* filterPolicyPtr = NULL;
    if (filterPolicy > 0)
    {
       filterPolicyPtr = leveldb::NewBloomFilterPolicy((size_t)filterPolicy);;
       options.filter_policy = filterPolicyPtr;
    }

    //options.compression = leveldb::kSnappyCompression;
    leveldb::Status status = leveldb::DB::Open(options, path, &db);
    env->ReleaseStringUTFChars(dbpath, path);

    if (!status.ok())
    {
        LOGI("can't open database,the err: %s", status.ToString().c_str());
        delete logger;
        delete cache;
        delete filterPolicyPtr;
        throwException(env, status);
        return 0;
    }
    else
    {
        LOGI("Opened database %s", path);
    }

    KCDBHolder* holder = new KCDBHolder(db, logger, cache, filterPolicyPtr);
    return reinterpret_cast<jlong>(holder);
}

static void nativeClose(JNIEnv* env, jclass clazz, jlong dbPtr)
{
  if (dbPtr != 0)
  {
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    LOGI("Database closed");

    if (holder)
    {
        if(holder->db)
            delete holder->db;
        if(holder->cache)
            delete holder->cache;
        if(holder->logger)
            delete holder->logger;
        if(holder->filterPolicy)
            delete holder->filterPolicy;

        delete holder;
    }

  }

}


static void nativePut(JNIEnv *env, jclass clazz, jlong dbPtr, jbyteArray keyObj, jbyteArray valObj, jboolean sync)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync == JNI_TRUE;

    size_t keyLen = env->GetArrayLength(keyObj);
    jbyte *keyBuf = env->GetByteArrayElements(keyObj, NULL);

    size_t valLen = env->GetArrayLength(valObj);
    jbyte *valBuf = env->GetByteArrayElements(valObj, NULL);

    leveldb::Status status = db->Put(writeOptions,
            leveldb::Slice((const char *) keyBuf, keyLen),
            leveldb::Slice((const char *) valBuf, valLen));

    env->ReleaseByteArrayElements(keyObj, keyBuf, JNI_ABORT);
    env->ReleaseByteArrayElements(valObj, valBuf, JNI_ABORT);

    if (!status.ok())
    {
        throwException(env, status);
    }
}

static void nativePutString(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jstring jValue)
{
	//LOGI("Putting a String ");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	const char* value = env->GetStringUTFChars(jValue, 0);

	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);
	env->ReleaseStringUTFChars(jValue, value);
	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
    }
}

static void nativePutBytes(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jbyteArray arr)
{
	//LOGI("Putting a Serializable ");
   KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
   leveldb::DB* db = holder->db;

	int len = env->GetArrayLength(arr);
	jbyte* data =  (jbyte*)env->GetPrimitiveArrayCritical(arr, 0);
	if (data == NULL) {
	    /* out of memory exception thrown */
		throwDBException(env, "OutOfMemory when trying to get bytes array for Serializable");
		return;
	}

	const char* key(env->GetStringUTFChars(jKey, 0));
	leveldb::Slice value(reinterpret_cast<char*>(data), len);

	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);

	env->ReleasePrimitiveArrayCritical(arr, data, 0);
	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
    }
}

static void nativePutShort(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jshort jValue)
{
	//LOGI("Putting a short");
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key(env->GetStringUTFChars(jKey, 0));
	leveldb::Slice value((char*) &jValue, sizeof(jshort));

	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);

	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
        //std::string err("Failed to put a short: " + status.ToString());
        //throwDBException(env, err.c_str());
    }
}

static void nativePutInt(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jint jVal)
{
	//LOGI("Putting an int");
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key(env->GetStringUTFChars(jKey, 0));
	leveldb::Slice value((char*) &jVal, sizeof(jint));

	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);

	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
        //std::string err("Failed to put a int: " + status.ToString());
        //throwDBException(env, err.c_str());
    }
}
static void nativePutBoolean(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jboolean jValue)
{
	//LOGI("Putting a boolean");
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key(env->GetStringUTFChars(jKey, 0));
	leveldb::Slice value((char*) &jValue, sizeof(jboolean));

	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);

	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
    }
}
static void nativePutDouble(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jdouble jVal)
{
	//LOGI("Putting a double");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key(env->GetStringUTFChars(jKey, 0));

	std::ostringstream oss;
	oss << std::setprecision(17) << jVal;
	std::string value = oss.str();
	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);

	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
    }
}
static void nativePutFloat(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jfloat jValue)
{
//	LOGI("Putting a float");
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key(env->GetStringUTFChars(jKey, 0));
	std::ostringstream oss;
	oss << std::setprecision(16) << jValue;
	std::string value = oss.str();

	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);

	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
    }
}
static void nativePutLong(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jlong jVal)
{
	//LOGI("Putting a long ");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key(env->GetStringUTFChars(jKey, 0));
	leveldb::Slice value((char*) &jVal, sizeof(jlong));

	leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);

	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
    }
}

static void nativeDelete(JNIEnv *env, jclass clazz, jlong dbPtr, jbyteArray keyObj, jboolean sync)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

    size_t keyLen = env->GetArrayLength(keyObj);
    jbyte *buffer = env->GetByteArrayElements(keyObj, NULL);

    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync == JNI_TRUE;

    leveldb::Status status = db->Delete(writeOptions, leveldb::Slice((const char *) buffer, keyLen));
    env->ReleaseByteArrayElements(keyObj, buffer, JNI_ABORT);

    if (!status.ok())
    {
        throwException(env, status);
    }
}
static void nativeDeleteByKeyString(JNIEnv *env, jclass clazz, jlong dbPtr, jstring jKey, jboolean sync)
{
	//LOGI("Deleting entry");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);

    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync == JNI_TRUE;

	leveldb::Status status = db->Delete(writeOptions, key);
	env->ReleaseStringUTFChars(jKey, key);

    if (!status.ok())
    {
        throwException(env, status);
    }
}

static void nativeWrite(JNIEnv *env, jclass clazz, jlong dbPtr, jlong batchPtr, jboolean sync)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

    leveldb::WriteOptions options;
    options.sync = sync == JNI_TRUE;

    leveldb::WriteBatch *batch = (leveldb::WriteBatch *) batchPtr;
    leveldb::Status status = db->Write(options, batch);
    if (!status.ok())
    {
        throwException(env, status);
    }
}

static jbyteArray nativeGet(JNIEnv * env, jclass clazz, jlong dbPtr, jlong snapshotPtr, jbyteArray keyObj)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

    leveldb::ReadOptions options = leveldb::ReadOptions();
    options.snapshot = reinterpret_cast<leveldb::Snapshot*>(snapshotPtr);

    size_t keyLen = env->GetArrayLength(keyObj);
    jbyte *buffer = env->GetByteArrayElements(keyObj, NULL);
    jbyteArray result;

    leveldb::Slice key = leveldb::Slice((const char *)buffer, keyLen);
    leveldb::Iterator* iter = db->NewIterator(options);
    iter->Seek(key);
    if (iter->Valid() && key == iter->key())
    {
        leveldb::Slice value = iter->value();
        size_t len = value.size();
        result = env->NewByteArray(len);
        env->SetByteArrayRegion(result, 0, len, (const jbyte *) value.data());
    }
    else
    {
        result = NULL;
    }

    env->ReleaseByteArrayElements(keyObj, buffer, JNI_ABORT);
    delete iter;

    return result;
}

static jbyteArray nativeGetByBuffer(JNIEnv * env, jclass clazz, jlong dbPtr, jlong snapshotPtr, jobject keyObj)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

    leveldb::ReadOptions options = leveldb::ReadOptions();
    options.snapshot = reinterpret_cast<leveldb::Snapshot*>(snapshotPtr);

    jint keyPos = env->CallIntMethod(keyObj, gByteBuffer_positionMethodID);
    jint keyLimit = env->CallIntMethod(keyObj, gByteBuffer_limitMethodID);
    jboolean keyIsDirect = env->CallBooleanMethod(keyObj, gByteBuffer_isDirectMethodID);
    jbyteArray keyArray;
    void* key;
    if (keyIsDirect)
    {
        key = env->GetDirectBufferAddress(keyObj);
        keyArray = NULL;
    }
    else
    {
        keyArray = (jbyteArray) env->CallObjectMethod(keyObj, gByteBuffer_arrayMethodID);
        key = (void*) env->GetByteArrayElements(keyArray, NULL);
    }

    jbyteArray result;
    leveldb::Slice keySlice = leveldb::Slice((const char *) key + keyPos, keyLimit - keyPos);
    leveldb::Iterator* iter = db->NewIterator(options);
    iter->Seek(keySlice);
    if (iter->Valid() && keySlice == iter->key())
    {
        leveldb::Slice value = iter->value();
        size_t len = value.size();
        result = env->NewByteArray(len);
        env->SetByteArrayRegion(result, 0, len, (const jbyte *) value.data());
    }
    else
    {
        result = NULL;
    }

    if (keyArray)
    {
        env->ReleaseByteArrayElements(keyArray, (jbyte*) key, JNI_ABORT);
    }

    delete iter;

    return result;
}
static jbyteArray nativeGetByKeyString(JNIEnv * env, jclass clazz, jlong dbPtr, jlong snapshotPtr, jstring jKey)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

    leveldb::ReadOptions options = leveldb::ReadOptions();
    options.snapshot = reinterpret_cast<leveldb::Snapshot*>(snapshotPtr);

    const char* key = env->GetStringUTFChars(jKey, 0);
    jbyteArray result;

    leveldb::Iterator* iter = db->NewIterator(options);
    iter->Seek(key);
    if (iter->Valid() && key == iter->key())
    {
        leveldb::Slice value = iter->value();
        size_t len = value.size();
        result = env->NewByteArray(len);
        env->SetByteArrayRegion(result, 0, len, (const jbyte *) value.data());
    }
    else
    {
        result = NULL;
    }

    env->ReleaseStringUTFChars(jKey, key);
    delete iter;

    return result;
}
static jbyteArray nativeGetBytes(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("Getting a byte array");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string data;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &data);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{
		int size = data.size();

		char* elems = const_cast<char*>(data.data());
		jbyteArray array = env->NewByteArray(size * sizeof(jbyte));
		env->SetByteArrayRegion(array, 0, size, reinterpret_cast<jbyte*>(elems));

		//LOGI("Successfully reading a byte array");
		return array;
	}
	else
	{
		//std::string err("Failed to get a byte array: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}

static jstring nativeGetString(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("Getting a String");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string value;
	leveldb::ReadOptions();
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{
		LOGI("Successfully reading a String");
		const char* re = value.c_str();
		return env->NewStringUTF(re);

	}
	else
	{
		//std::string err("Failed to get a String: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}
static jshort nativeGetShort(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("Getting a short");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string data;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &data);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{
		if (sizeof(short) <= data.length())
		{
			LOGI("Successfully reading a short");

			const char* bytes = data.data();
			short ret = 0;
			ret = (unsigned char)bytes[1];
			ret = (ret << 8) + (unsigned char)bytes[0];

			return ret;
		}
		else
		{
			throwDBException(env, "Failed to get a short");
			return NULL;
		}

	}
	else
	{
		//std::string err("Failed to get a short: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}

static jint nativeGetInt(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
    //LOGI("Getting an int");
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string data;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &data);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{
		if (sizeof(int) <= data.length())
		{
			LOGI("Successfully reading an int");

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
			throwDBException(env, "Failed to get an int");
			return NULL;
		}
	}
	else
	{
		//std::string err("Failed to get an int: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}

static jboolean nativeGetBoolean(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("Getting a boolean");
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string data;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &data);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{
		if (sizeof(bool) <= data.length())
		{
			//LOGI("Successfully reading a boolean");
			return data.data()[0];

		}
		else
		{
			throwDBException(env, "Failed to get a boolean");
			return NULL;
		}
	}
	else
	{
		//std::string err("Failed to get a boolean: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}

static jdouble nativeGetDouble(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("Getting a double");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string data;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &data);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{// we can't use data.length() here to make sure of the size of float since it was encoded as string
		double d = atof(data.c_str());
		//LOGI("Successfully reading a double");
		return d;
	}
	else
	{
		//std::string err("Failed to get a double: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}

}

static jfloat nativeGetFloat(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("Getting a float");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string data;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &data);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{// we can't use data.length() here to make sure of the size of float since it was encoded as string
		//LOGI("Successfully reading a float");
		float f = atof(data.c_str());
		return f;

	}
	else
	{
		//std::string err("Failed to get a float: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}

static jlong nativeGetLong(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("Getting a long");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string data;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &data);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{
		if (sizeof(long) <= data.length())
		{
			LOGI("Successfully reading a long");
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
			throwDBException(env, "Failed to get a long");
			return NULL;
		}

	}
	else
	{
		//std::string err("Failed to get a long: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}

static jboolean nativeExists(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jKey)
{
	//LOGI("does key exists");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* key = env->GetStringUTFChars(jKey, 0);
	std::string value;
	leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);

	env->ReleaseStringUTFChars(jKey, key);

	if (status.ok())
	{
		//LOGI("Key Found ");
		return JNI_TRUE;

	}
	else if (status.IsNotFound())
	{
		//LOGI("Key Not Found ");
		return JNI_FALSE;

	}
	else
	{
		//std::string err("Failed to check if a key exists: " + status.ToString());
		//throwDBException(env, err.c_str());
		throwException(env, status);
		return NULL;
	}
}

static jobjectArray nativeFindKeys(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jPrefix, jint offset, jint limit)
{
	//LOGI("find keys");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* prefix = env->GetStringUTFChars(jPrefix, 0);

	std::vector<std::string> result;
	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

	int count = 0;
	for (it->Seek(prefix); count < (offset + limit) && it->Valid() && it->key().starts_with(prefix); it->Next())
	{
        if (count >= offset)
        {
    		result.push_back(it->key().ToString());
    	}
        ++count;
	}

	std::vector<std::string>::size_type n = result.size();
	jobjectArray ret= (jobjectArray)env->NewObjectArray(n,
		         env->FindClass("java/lang/String"),
		         NULL);

	jstring str;
	for (int i=0; i<n ; i++)
	{
		str = env->NewStringUTF(result[i].c_str());
		env->SetObjectArrayElement(ret, i, str);
		env->DeleteLocalRef(str);
	}

	env->ReleaseStringUTFChars(jPrefix, prefix);
	delete it;

	return ret;
}

static jint nativeCountKeys(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jPrefix)
{
	//LOGI("count keys");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* prefix = env->GetStringUTFChars(jPrefix, 0);

	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

	jint count = 0;
	for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next())
	{
    	++count;
    }

    env->ReleaseStringUTFChars(jPrefix, prefix);
    delete it;

    return count;
}

static jobjectArray nativeFindKeysBetween(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jStartPrefix, jstring jEndPrefix, jint offset, jint limit)
{
	//LOGI("find keys between range");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* startPrefix = env->GetStringUTFChars(jStartPrefix, 0);
	const char* endPrefix = env->GetStringUTFChars(jEndPrefix, 0);

	std::vector<std::string> result;
	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

	int count = 0;
	for (it->Seek(startPrefix); count < (offset + limit) && it->Valid() && it->key().compare(endPrefix) <= 0; it->Next())
	{
		if (count >= offset)
		{
    		result.push_back(it->key().ToString());
    	}
    	++count;
	}

	std::vector<std::string>::size_type n = result.size();
	jobjectArray ret= (jobjectArray)env->NewObjectArray(n,
		         env->FindClass("java/lang/String"),
		         env->NewStringUTF(""));

	jstring str;
	for (int i=0; i<n ; i++)
	{
		str = env->NewStringUTF(result[i].c_str());
		env->SetObjectArrayElement(ret, i, str);
		env->DeleteLocalRef(str);
	}

	env->ReleaseStringUTFChars(jStartPrefix, startPrefix);
	env->ReleaseStringUTFChars(jEndPrefix, endPrefix);
	delete it;

	return ret;
}
static jint nativeCountKeysBetween(JNIEnv * env, jclass clazz, jlong dbPtr, jstring jStartPrefix, jstring jEndPrefix)
{
	//LOGI("count keys between range");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	const char* startPrefix = env->GetStringUTFChars(jStartPrefix, 0);
	const char* endPrefix = env->GetStringUTFChars(jEndPrefix, 0);

	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

	jint count = 0;
	for (it->Seek(startPrefix); it->Valid() && it->key().compare(endPrefix) <= 0; it->Next())
	{
    	++count;
	}

	env->ReleaseStringUTFChars(jStartPrefix, startPrefix);
	env->ReleaseStringUTFChars(jEndPrefix, endPrefix);
	delete it;

	return count;
}


static jlong nativeIterator(JNIEnv* env, jclass clazz, jlong dbPtr, jlong snapshotPtr, jboolean fillCache)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

    leveldb::ReadOptions options = leveldb::ReadOptions();
    options.snapshot = reinterpret_cast<leveldb::Snapshot*>(snapshotPtr);
    options.fill_cache = (bool) fillCache;

    leveldb::Iterator *iter = db->NewIterator(options);
    return reinterpret_cast<jlong>(iter);
}
static jlong nativeFindKeysIterator(JNIEnv* env, jclass clazz, jlong dbPtr, jstring jPrefix, jboolean reverse)
{
	//LOGI("find keys iterator");
	KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;

	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

	if (jPrefix)
	{
		const char* prefix = env->GetStringUTFChars(jPrefix, 0);
		LOGI("(%p) Seeking prefix: %s", it, prefix);
		it->Seek(prefix);
	    env->ReleaseStringUTFChars(jPrefix, prefix);
	}
	else if (reverse)
	{
		it->SeekToLast();
	}
	else
	{
		it->SeekToFirst();
	}

	// When seeking in a leveldb iterator, if the key does not exists, it is positioned to the key
	// immediately *after* what we are seeking or invalid. In the case of a reverse iterator, we
	// want the key immediately *before* or the last.
	if (reverse)
	 {
		if (!it->Valid())
		{
			it->SeekToLast();
		}
		else if (jPrefix)
		{
			const char* prefix = env->GetStringUTFChars(jPrefix, 0);
			if (it->key().compare(prefix) > 0)
			{
				it->Prev();
			}
			env->ReleaseStringUTFChars(jPrefix, prefix);
		}
	}

	return (jlong) it;
}


static jlong nativeGetSnapshot(JNIEnv *env, jclass clazz, jlong dbPtr)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;
    const leveldb::Snapshot* snapshot = db->GetSnapshot();
    return reinterpret_cast<jlong>(snapshot);
}

static void nativeReleaseSnapshot(JNIEnv *env, jclass clazz, jlong dbPtr, jlong snapshotPtr)
{
    KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
    leveldb::DB* db = holder->db;
    const leveldb::Snapshot *snapshot = reinterpret_cast<leveldb::Snapshot*>(snapshotPtr);
    db->ReleaseSnapshot(snapshot);
}

static void nativeDestroy(JNIEnv *env, jclass clazz, jstring dbpath)
{
    const char* path = env->GetStringUTFChars(dbpath,0);
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = DestroyDB(path, options);
    if (!status.ok())
    {
        throwException(env, status);
    }
}
static jbyteArray nativeGetProperty(JNIEnv *env, jclass clazz, jlong dbPtr, jbyteArray key)
{
  KCDBHolder* holder = reinterpret_cast<KCDBHolder*>(dbPtr);
  leveldb::DB* db = holder->db;

  const char* keyData = (char*) env->GetByteArrayElements(key, 0);

  leveldb::Slice keySlice (keyData, (size_t) env->GetArrayLength(key));

  leveldb::ReadOptions readOptions;

  std::string value;

  bool ok = db->GetProperty(keySlice, &value);

  env->ReleaseByteArrayElements(key, (jbyte*) keyData, 0);

  if (ok)
  {
     if (value.length() < 1)
     {
        return 0;
     }

    jbyteArray retval = env->NewByteArray(value.length());

    env->SetByteArrayRegion(retval, 0, value.length(), (jbyte*) value.data());

    return retval;
  }

  return 0;
}

static void nativeRepair(JNIEnv* env, jclass clazz, jstring dbpath)
{
  const char *nativePath = env->GetStringUTFChars(dbpath, 0);

  leveldb::Status status = leveldb::RepairDB(nativePath, leveldb::Options());

  env->ReleaseStringUTFChars(dbpath, nativePath);

  throwException(env, status);
}

static JNINativeMethod sMethods[] =
{
        { "nativeOpen", "(Ljava/lang/String;ZIIIZZZI)J", (void*) nativeOpen },
        { "nativeClose", "(J)V", (void*) nativeClose },
        { "nativePut", "(J[B[BZ)V", (void*) nativePut },
        { "nativePut", "(JLjava/lang/String;Ljava/lang/String;)V", (void*) nativePutString },
        { "nativePut", "(JLjava/lang/String;[B)V", (void*) nativePutBytes },
        { "nativePut", "(JLjava/lang/String;S)V", (void*) nativePutShort },
        { "nativePut", "(JLjava/lang/String;I)V", (void*) nativePutInt },
        { "nativePut", "(JLjava/lang/String;Z)V", (void*) nativePutBoolean },
        { "nativePut", "(JLjava/lang/String;D)V", (void*) nativePutDouble },
        { "nativePut", "(JLjava/lang/String;F)V", (void*) nativePutFloat },
        { "nativePut", "(JLjava/lang/String;J)V", (void*) nativePutLong },
        { "nativeDelete", "(J[BZ)V", (void*) nativeDelete },
        { "nativeDelete", "(JLjava/lang/String;Z)V", (void*) nativeDeleteByKeyString },
        { "nativeWrite", "(JJZ)V", (void*) nativeWrite },
        { "nativeGet", "(JJ[B)[B", (void*) nativeGet },
        { "nativeGet", "(JJLjava/nio/ByteBuffer;)[B", (void*) nativeGetByBuffer },
        { "nativeGet", "(JJLjava/lang/String;)[B", (void*) nativeGetByKeyString },
        { "nativeGetBytes", "(JLjava/lang/String;)[B", (void*) nativeGetBytes },
        { "nativeGetString", "(JLjava/lang/String;)Ljava/lang/String;", (void*) nativeGetString },
        { "nativeGetShort", "(JLjava/lang/String;)S", (void*) nativeGetShort },
        { "nativeGetInt", "(JLjava/lang/String;)I", (void*) nativeGetInt },
        { "nativeGetBoolean", "(JLjava/lang/String;)Z", (void*) nativeGetBoolean },
        { "nativeGetDouble", "(JLjava/lang/String;)D", (void*) nativeGetDouble },
        { "nativeGetFloat", "(JLjava/lang/String;)F", (void*) nativeGetFloat },
        { "nativeGetLong", "(JLjava/lang/String;)J", (void*) nativeGetLong },
        { "nativeExists", "(JLjava/lang/String;)Z", (void*) nativeExists },
        { "nativeFindKeys", "(JLjava/lang/String;II)[Ljava/lang/String;", (void*) nativeFindKeys },
        { "nativeCountKeys", "(JLjava/lang/String;)I", (void*) nativeCountKeys },
        { "nativeFindKeysBetween", "(JLjava/lang/String;Ljava/lang/String;II)[Ljava/lang/String;", (void*) nativeFindKeysBetween },
        { "nativeCountKeysBetween", "(JLjava/lang/String;Ljava/lang/String;)I", (void*) nativeCountKeysBetween },
        { "nativeIterator", "(JJZ)J", (void*) nativeIterator },
        { "nativeFindKeysIterator", "(JLjava/lang/String;Z)J", (void*) nativeFindKeysIterator },
        { "nativeGetSnapshot", "(J)J", (void*) nativeGetSnapshot },
        { "nativeReleaseSnapshot", "(JJ)V", (void*) nativeReleaseSnapshot },
        { "nativeDestroy", "(Ljava/lang/String;)V", (void*) nativeDestroy },
        { "nativeGetProperty", "(J[B)[B", (void*) nativeGetProperty },
        { "nativeRepair", "(Ljava/lang/String;)V", (void*) nativeRepair }
};

int register_DB(JNIEnv *env)
 {
    jclass clazz = env->FindClass("com/kercer/kerdb/jnibridge/KCDBNative");
    if (!clazz)
    {
        LOGE("Can't find class com.kercer.kerdb.jnibridge.KCDBNative");
        return 0;
    }

    return env->RegisterNatives(clazz, sMethods, NELEM(sMethods));
}
