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

	//����systemInfo�������512�ֽڿռ�
	//errmsgΪ������Ϣ,�ɼ�������ȷ��Ϊ "QDP get sys_info correct"
	//bTest: �Ƿ�Ϊ����汾��Ĭ��Ϊ�����汾
	//����ֵ��-1 - �ɼ����󣬴���0 - ���زɼ����ݵĳ���
	//����ʹ���ڴ渳ֵ
	QDPAPI_EXPORT int QDGetLocalSystemInfo(char *systemInfo, char *errmsg, bool bTest=false);

#ifdef __cplusplus
}
#endif
#endif
////////////////////////////////
