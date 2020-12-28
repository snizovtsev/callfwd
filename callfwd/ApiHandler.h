#pragma once

#include <folly/Memory.h>

class PhoneMapping;

namespace proxygen {
class RequestHandlerFactory;
}

std::unique_ptr<proxygen::RequestHandlerFactory>
makeApiHandlerFactory(std::shared_ptr<PhoneMapping> db);
