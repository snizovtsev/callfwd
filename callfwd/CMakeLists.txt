set(SOURCES
  PhoneMapping.cpp
  PhoneMapping.h
  ApiHandler.cpp
  ApiHandler.h
  ApiServer.cpp
  )

add_executable(callfwd ${SOURCES})
target_link_libraries(callfwd
  proxygen::proxygenhttpserver
  TBB::tbb
  )

add_subdirectory(test)
