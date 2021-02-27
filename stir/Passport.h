#ifndef STIR_PASSPORT_H
#define STIR_PASSPORT_H

#include "StirApiTypes.h"

std::string makePassport(const SigningRequest &req);
VerificationResponse verifyPassport(const VerificationRequest &request);

#endif
