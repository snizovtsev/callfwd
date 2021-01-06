#ifndef CALLFWD_CONTROL_H
#define CALLFWD_CONTROL_H

#include <memory>
#include "PhoneMapping.h"

bool loadMappingFile(const char* fname);
std::shared_ptr<PhoneMapping> getPhoneMapping();
void startControlSocket();

#endif // CALLFWD_CONTROL_H
