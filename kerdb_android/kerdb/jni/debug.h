
//Debug routine
#include <android/log.h>

#define DEBUG false
#define  LOG_TAG    "kerdb"
#define  LOGI(...)  if(DEBUG) __android_log_print(ANDROID_LOG_INFO,LOG_TAG,__VA_ARGS__)
#define  LOGE(...)  if(DEBUG) __android_log_print(ANDROID_LOG_ERROR,LOG_TAG,__VA_ARGS__)
