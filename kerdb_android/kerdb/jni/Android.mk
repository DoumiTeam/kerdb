
LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)
include $(LOCAL_PATH)/common.mk

LOCAL_MODULE := kerdb
LOCAL_C_INCLUDES := $(C_INCLUDES)
LOCAL_CFLAGS := -DLEVELDB_PLATFORM_ANDROID -std=gnu++0x -g -w
LOCAL_SRC_FILES := $(SOURCES) $(RELATIVE_PATH)/port/port_android.cc kerdb.cpp
LOCAL_SRC_FILES +=kerdbjni.cpp KCDB.cpp KCIterator.cpp KCWriteBatch.cpp
LOCAL_LDLIBS +=  -llog
include $(BUILD_SHARED_LIBRARY)
