include( XRootDCommon )

add_executable(
  xrdec_test
  xrdec_test.cc )

target_link_libraries(
  xrdec_test
  XrdEc
  XrdCl
  XrdPosix
  XrdXml
  XrdAppUtils
  dl
  pthread )
  
#------------------------------------------------------------------------------
#
#------------------------------------------------------------------------------ 
add_library(
  epn_load_gen_lib
  SHARED
  PlacementHandler.cc   PlacementHandler.hh
  StreamSimulator.cc    StreamSimulator.hh
)

target_link_libraries(
  epn_load_gen_lib
  XrdEc
  XrdCl
  XrdPosix
  XrdXml
  XrdAppUtils
  dl
  pthread)

set_target_properties(
  epn_load_gen_lib
  PROPERTIES
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES ""
  VERSION   0
  SOVERSION 0 )

#------------------------------------------------------------------------------  
#
#------------------------------------------------------------------------------
add_executable(
  epn_load_gen
  epn_load_gen.cc )
    
target_link_libraries(
  epn_load_gen
  epn_load_gen_lib
  XrdEc
  XrdCl
  XrdPosix
  XrdXml
  XrdAppUtils
  dl
  pthread )
