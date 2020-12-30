proxygen_add_test(TARGET PhoneMappingTests
  SOURCES
    PhoneMappingTest.cpp
    ../PhoneMapping.cpp
  DEPENDS
    testmain
    TBB::tbb
)
