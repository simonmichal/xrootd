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
