set(SOURCES
  EchoHandler.cpp
  EchoHandler.h
  EchoServer.cpp
  EchoStats.h
  )

add_executable(echo ${SOURCES})
target_link_libraries(echo proxygen::proxygenhttpserver)

add_subdirectory(test)
