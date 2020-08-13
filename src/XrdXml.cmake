
include( XRootDCommon )

if ( LIBXML2_FOUND )
   set( XRDXML2_READER_FILES
        XrdXml/XrdXmlRdrXml2.cc
        XrdXml/XrdXmlRdrXml2.hh )
   set( XRDXML2_LIBRARIES ${LIBXML2_LIBRARIES} )
   
   include_directories( ${LIBXML2_INCLUDE_DIR} )
   
else()
   set( XRDXML2_READER_FILES "" )
   set( XRDXML2_LIBRARIES "" )
endif()

#-------------------------------------------------------------------------------
# Shared library version
#-------------------------------------------------------------------------------
set( XRD_XML_VERSION   3.0.0 )
set( XRD_XML_SOVERSION 3 )
set( XRD_XML_PRELOAD_VERSION   2.0.0 )
set( XRD_XML_PRELOAD_SOVERSION 2 )

#-------------------------------------------------------------------------------
# The XrdXml library
#-------------------------------------------------------------------------------
add_library(
  XrdXml
  SHARED
  XrdXml/tinystr.cpp               XrdXml/tinystr.h
  XrdXml/tinyxml.cpp               XrdXml/tinyxml.h
  XrdXml/tinyxmlerror.cpp
  XrdXml/tinyxmlparser.cpp
  XrdXml/XrdXmlMetaLink.cc         XrdXml/XrdXmlMetaLink.hh
  XrdXml/XrdXmlRdrTiny.cc          XrdXml/XrdXmlRdrTiny.hh
  XrdXml/XrdXmlReader.cc           XrdXml/XrdXmlReader.hh
  ${XRDXML2_READER_FILES} )

target_link_libraries(
  XrdXml
  XrdUtils
  ${XRDXML2_LIBRARIES}
  pthread )
# INCLUDE_DIRECTORIES /usr/include/libxml2

set_target_properties(
  XrdXml
  PROPERTIES
  VERSION   ${XRD_XML_VERSION}
  SOVERSION ${XRD_XML_SOVERSION}
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES "" )

if ( LIBXML2_FOUND )

endif()

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  TARGETS XrdXml
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )
