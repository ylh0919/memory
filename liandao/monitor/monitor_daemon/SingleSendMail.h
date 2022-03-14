#ifndef KUNGFU_SINGLESENDMAIL_H
#define KUNGFU_SINGLESENDMAIL_H

#include<iostream>
#include<string>
#include<chrono>
#include<time.h> 
#include<map>
#include<cpr.h>
#include<document.h>
#include<openssl/hmac.h>
#include<openssl/sha.h>
#include<openssl/rsa.h>
#include<openssl/pem.h>
#include<openssl/evp.h>
using namespace std; 


std::string url_encode(const char *str);
const char *get_utc();
std::string getTimestamp();
std::string generate(const std::string & src,const std::string & secret);
void SingleSendMail(std::string AccessKeySecret,std::string AccessKeyId,std::string AccountName,std::string Subject,std::string TextBody,std::string ToAddress);

#endif 