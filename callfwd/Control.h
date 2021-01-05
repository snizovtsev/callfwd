#ifndef CALLFWD_CONTROL_H
#define CALLFWD_CONTROL_H

#include <memory>
#include "PhoneMapping.h"

void loadMappingFile(const char* fname);
std::shared_ptr<PhoneMapping> getPhoneMapping();

#endif // CALLFWD_CONTROL_H
