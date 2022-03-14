#ifndef __EXPORT_SEC_API_H
#if defined(WINDOWS) || defined(WIN32)
#ifdef QDPAPI_EXPORT
#define QDPAPI_EXPORT __declspec(dllexport)
#else
#define QDPAPI_EXPORT __declspec(dllimport)
#endif
#else
#define QDPAPI_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

	//建议systemInfo申请大于512字节空间
	//errmsg为错误信息,采集加密正确则为 "QDP get sys_info correct"
	//bTest: 是否为评测版本，默认为生产版本
	//返回值：-1 - 采集错误，大于0 - 返回采集数据的长度
	//建议使用内存赋值
	QDPAPI_EXPORT int QDGetLocalSystemInfo(char *systemInfo, char *errmsg, bool bTest=false);

#ifdef __cplusplus
}
#endif
#endif
////////////////////////////////
