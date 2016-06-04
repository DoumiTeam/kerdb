#include "kerdbjni.h"

extern int register_DB(JNIEnv *env);
extern int register_WriteBatch(JNIEnv *env);
extern int register_Iterator(JNIEnv *env);

jint throwException(JNIEnv* env, leveldb::Status status)
 {
    const char* exceptionClass;

    if (status.IsNotFound())
    {
        exceptionClass = "com/kercer/kerdb/jnibridge/exception/KCNotFoundException";
    }
    else if (status.IsCorruption())
    {
        exceptionClass = "com/kercer/kerdb/jnibridge/exception/KCDBCorruptException";
    }
    else if (status.IsIOError())
    {
        exceptionClass = "java/io/IOException";
    }
    else
    {
        exceptionClass = "com/kercer/kerdb/jnibridge/exception/KCDBException";
    }

    jclass clazz = env->FindClass(exceptionClass);
    if (!clazz)
    {
        LOGE("Can't find exception class %s", exceptionClass);
        return -1;
    }

    return env->ThrowNew(clazz, status.ToString().c_str());
}

void throwDBException(JNIEnv *env, const char* msg)
{
	LOGE("throwException %s", msg);
	jclass dbExceptionClazz = env->FindClass("com/kercer/kerdb/jnibridge/exception/KCDBException");
	if ( NULL == dbExceptionClazz)
	{
		// FindClass already threw an exception such as NoClassDefFoundError.
		env->Throw(env->ExceptionOccurred());
		return;
	}
	 env->ThrowNew(dbExceptionClazz, msg);
}


jint __JNI_OnLoad(JavaVM* vm, void *reserved)
{
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6) != JNI_OK) {
        return -1;
    }

    register_DB(env);
    register_WriteBatch(env);
    register_Iterator(env);

    return JNI_VERSION_1_6;
}
