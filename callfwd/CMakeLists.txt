set(SOURCES
  PhoneMapping.cpp
  PhoneMapping.h
  ApiHandler.cpp
  ApiServer.cpp
  )

add_executable(callfwd ${SOURCES})
target_link_libraries(callfwd
  proxygen::proxygenhttpserver
  TBB::tbb
  )

add_subdirectory(test)
