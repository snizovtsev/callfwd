set(SOURCES
  PhoneMapping.cpp
  PhoneMapping.h
  ApiHandler.cpp
  SipHandler.cpp
  Control.cpp
  CallFwd.cpp
  )

add_executable(callfwd ${SOURCES})
target_link_libraries(callfwd
  proxygen::proxygenhttpserver
  TBB::tbb
  osips_parser
  )

add_executable(sip-test SipTest.cpp)
target_link_libraries(sip-test
  Folly::folly
  glog::glog
  osips_parser
  )

add_subdirectory(test)
