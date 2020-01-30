
include( XRootDCommon )

set( LIB_XRD_EC  XrdEc-${PLUGIN_VERSION} )
#set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x" )

include_directories( ${CMAKE_SOURCE_DIR}/src/qclient/include/ ${CMAKE_SOURCE_DIR}/src/qclient/src/fmt/include/)

#-------------------------------------------------------------------------------
# xrdcopy
#-------------------------------------------------------------------------------
if( NOT XRDCL_ONLY )

add_executable(
  xrdcl-tls
  xrdcl-tls.cc )
  
target_link_libraries(
  xrdcl-tls
  XrdCl
  XrdPosix
  XrdXml
  XrdAppUtils
  XrdEc
  uuid
  dl
  pthread )

add_executable(
  xrdsrv-tls
  xrdsrv-tls.cc )
  
target_link_libraries(
  xrdsrv-tls
  XrdUtils
  ${OPENSSL_LIBRARIES}
  pthread )
  
add_executable(
  xrdec2_test
  xrdec2_test.cc )
  
target_link_libraries(
  xrdec2_test
  XrdCl
  XrdPosix
  XrdXml
  XrdAppUtils
  XrdEc2
  uuid
  dl
  pthread )

endif()
