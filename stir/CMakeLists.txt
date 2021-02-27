pkg_check_modules(UUID REQUIRED uuid IMPORTED_TARGET)

add_library(stir
  StirApiTypes.cpp
  StirApiHandler.cpp
  Passport.cpp
  )
target_link_libraries(stir
  proxygen::proxygenhttpserver
  PkgConfig::UUID
  )

add_executable(stird Stir.cpp)
target_link_libraries(stird stir)

add_executable(sandbox Sandbox.cpp)
target_link_libraries(sandbox stir)

add_subdirectory(test)
