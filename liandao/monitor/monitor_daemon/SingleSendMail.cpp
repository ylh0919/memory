#include "SingleSendMail.h"


char buf[32];

void SingleSendMail(std::string AccessKeySecret,std::string AccessKeyId,std::string AccountName,std::string Subject,std::string TextBody,std::string ToAddress)
{
	std::string HTTPMethod = "GET";
	std::map<std::string,std::string> map_param;
	std::string Timestamp = get_utc();
	std::string SignatureNonce = getTimestamp();
	map_param.insert(make_pair("AccessKeyId",AccessKeyId));
	map_param.insert(make_pair("AccountName",AccountName));
	map_param.insert(make_pair("Action","SingleSendMail"));
	map_param.insert(make_pair("AddressType","1"));
	map_param.insert(make_pair("Format","JSON"));	
	map_param.insert(make_pair("RegionId","cn-hangzhou"));
	map_param.insert(make_pair("ReplyToAddress","false"));
	map_param.insert(make_pair("SignatureMethod","HMAC-SHA1"));
	map_param.insert(make_pair("SignatureNonce",SignatureNonce));		
	map_param.insert(make_pair("SignatureVersion","1.0"));		
	map_param.insert(make_pair("Subject",Subject));		
	map_param.insert(make_pair("TextBody",TextBody));
	map_param.insert(make_pair("Timestamp",Timestamp));
	map_param.insert(make_pair("ToAddress",ToAddress));	
	map_param.insert(make_pair("Version","2015-11-23"));	

	std::string CanonicalizedQueryString="";
	for(auto it = map_param.begin();it != map_param.end();)
	{
		CanonicalizedQueryString = CanonicalizedQueryString + url_encode(it->first.c_str()) + "=" + url_encode(it->second.c_str());
		it++;
		if(it!=map_param.end())
			CanonicalizedQueryString += "&";
	}
	
	std::string StringToSign= HTTPMethod + "&" + "%2F" + "&" + url_encode(CanonicalizedQueryString.c_str());
	std::string Key = AccessKeySecret + "&";
	std::string Signature = generate(StringToSign,Key);
	cpr::Parameters param;
	param.AddParameter(cpr::Parameter("Signature",Signature)); 
	for(auto it = map_param.begin();it != map_param.end();it++)
		param.AddParameter(cpr::Parameter(it->first,it->second)); 
	 
	auto r = cpr::Get(cpr::Url{"http://dm.aliyuncs.com"},param);
 //   cout<< " (response.status_code) " << r.status_code << endl
	//    << " (response.error.message) " << r.error.message << endl
	//	<< " (response.text) " << r.text.c_str() <<endl;
}
std::string url_encode(const char *str) 
{
	char *encstr, buf[2+1];
	unsigned char c;
	int i, j;

	if(str == NULL) return NULL;
	if((encstr = (char *)malloc((strlen(str) * 3) + 1)) == NULL) 
			return NULL;

	for(i = j = 0; str[i]; i++) 
	{
		c = (unsigned char)str[i];
		if((c >= '0') && (c <= '9')) encstr[j++] = c;
		else if((c >= 'A') && (c <= 'Z')) encstr[j++] = c;
		else if((c >= 'a') && (c <= 'z')) encstr[j++] = c;
		else if((c == '-') || (c == '_') || (c == '.') || (c == '~') ) 
			encstr[j++] = c;
		else{
			sprintf(buf, "%02X", c);
			encstr[j++] = '%';
			encstr[j++] = buf[0];
			encstr[j++] = buf[1];
		}
	}
	encstr[j] = '\0';
    std::string retStr(encstr);
    free(encstr);
	return retStr;
}
const char *get_utc()
{
	struct tm *utc; 
	time_t t; 
	t=time(NULL); 
	utc=gmtime(&t); 
	strftime(buf,32, "%Y-%m-%d %H:%M:%S", utc);
	buf[10]='T';
	buf[19]='Z';
	buf[20]='\0';
	return buf;
}
std::string generate(const std::string & src,const std::string & secret)
{
  unsigned char md[EVP_MAX_BLOCK_LENGTH];
  unsigned int mdLen = EVP_MAX_BLOCK_LENGTH;

  if(HMAC(EVP_sha1(), secret.c_str(), secret.size(),reinterpret_cast<const unsigned char*>(src.c_str()), src.size(),md, &mdLen) == nullptr)
    return std::string();

  char encodedData[100];
  EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encodedData), md, mdLen);
  return encodedData;
}
std::string getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(timestamp);
}