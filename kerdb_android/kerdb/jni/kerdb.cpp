

#include <jni.h>
#include <stdlib.h>
#include "debug.h";


extern jint __JNI_OnLoad(JavaVM* vm, void *reserved);


JNIEXPORT jint JNI_OnLoad(JavaVM* vm, void* reserved) {
	LOGI("JVM is loading");
	return __JNI_OnLoad(vm, reserved);
}

// the class loader containing the native library is garbage collected,  perform cleanup operations
void JNI_OnUnload(JavaVM *vm, void *reserved) {
	LOGI("JVM is unloading");
}


