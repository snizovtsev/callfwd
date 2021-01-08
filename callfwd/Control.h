#ifndef CALLFWD_CONTROL_H
#define CALLFWD_CONTROL_H

#include <memory>
#include <folly/IPAddress.h>
#include "PhoneMapping.h"

bool loadMappingFile(const char* fname);
std::shared_ptr<PhoneMapping> getPhoneMapping();
void startControlSocket();
int checkACL(const folly::IPAddress &peer);

#endif // CALLFWD_CONTROL_H
