include( XRootDCommon )
include( ExternalProject )

include_directories( XrdEc ${CMAKE_SOURCE_DIR}/src/qclient/include/ ${CMAKE_SOURCE_DIR}/src/qclient/src/fmt/include/)

#-------------------------------------------------------------------------------
# Build isa-l
#-------------------------------------------------------------------------------

set(MAKEOPTIONS "")
if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "i386" OR "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "i686")
    set(MAKEOPTIONS "arch=32")
endif()

EXECUTE_PROCESS(
     COMMAND git ls-remote --tags https://github.com/01org/isa-l
     COMMAND awk "{print $2}"
     COMMAND grep -v {}
     COMMAND awk -F "/" "{print $3}"
     COMMAND tail -1 
     OUTPUT_VARIABLE ISAL_VERSION
)

MESSAGE( STATUS "Building ISAL: ${ISAL_VERSION}" )

set( ISAL_BUILDDIR "${CMAKE_BINARY_DIR}/isal/build" )
set( ISAL_INCDIR   "${CMAKE_BINARY_DIR}/isal/include" )
set( ISAL_LIBDIR   "${CMAKE_BINARY_DIR}/isal/lib" )

set( ISAL_HEADERS 
	 ${ISAL_BUILDDIR}/include/crc64.h  
	 ${ISAL_BUILDDIR}/include/crc.h  
	 ${ISAL_BUILDDIR}/include/erasure_code.h  
	 ${ISAL_BUILDDIR}/include/gf_vect_mul.h  
	 ${ISAL_BUILDDIR}/include/igzip_lib.h  
	 ${ISAL_BUILDDIR}/include/mem_routines.h  
	 ${ISAL_BUILDDIR}/include/multibinary.asm  
	 ${ISAL_BUILDDIR}/include/raid.h  
	 ${ISAL_BUILDDIR}/include/reg_sizes.asm  
	 ${ISAL_BUILDDIR}/include/test.h  
	 ${ISAL_BUILDDIR}/include/types.h
)

set( ISAL_LIBS 
     ${ISAL_BUILDDIR}/.libs/libisal.so 
     ${ISAL_BUILDDIR}/.libs/libisal.so.2 
     ${ISAL_BUILDDIR}/.libs/libisal.so.2.0.28
)

ExternalProject_add(
        isa-l
        SOURCE_DIR          ${ISAL_BUILDDIR}
        BUILD_IN_SOURCE     1
        GIT_REPOSITORY      https://github.com/01org/isa-l.git
        GIT_TAG             ${ISAL_VERSION}
        CONFIGURE_COMMAND   ./autogen.sh COMMAND ./configure
        BUILD_COMMAND       make ${MAKEOPTIONS}
        INSTALL_COMMAND     mkdir -p  ${ISAL_INCDIR}/isa-l
        COMMAND             mkdir -p  ${ISAL_LIBDIR}
        COMMAND             cp ${ISAL_HEADERS}           ${ISAL_INCDIR}/isa-l
        COMMAND             cp ${ISAL_BUILDDIR}/isa-l.h  ${ISAL_INCDIR}/isa-l
        COMMAND             cp ${ISAL_LIBS}              ${ISAL_LIBDIR}
)

link_directories( ${ISAL_LIBDIR} )
include_directories( ${ISAL_INCDIR} )
#add_dependencies


#-------------------------------------------------------------------------------
# Modules
#-------------------------------------------------------------------------------
set( LIB_XRD_EC  XrdEc-${PLUGIN_VERSION} )

#-------------------------------------------------------------------------------
# The XrdEc module
#-------------------------------------------------------------------------------
add_library(
  ${LIB_XRD_EC}
  MODULE
  XrdEcDataObject.hh           XrdEcDataObject.cc
  XrdEcConfig.hh
  XrdEcRedundancyProvider.hh   XrdEcRedundancyProvider.cc 
  XrdEcUtilities.hh            XrdEcUtilities.cc
  XrdEcRandomRead.hh           XrdEcRandomRead.cc
  XrdEcWriteBlock.hh           XrdEcWriteBlock.cc
  XrdEcReadBlock.hh            XrdEcReadBlock.cc
  XrdEcTruncate.hh             XrdEcTruncate.cc
  XrdEcScheduleHandler.hh      XrdEcScheduleHandler.cc
) 

target_link_libraries(
  ${LIB_XRD_EC}
  XrdFfs
  XrdPosix
  XrdCl
  XrdUtils
  isal
)

set_target_properties(
  ${LIB_XRD_EC}
  PROPERTIES
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES "" )
  
add_dependencies( ${LIB_XRD_EC} isa-l )

#-------------------------------------------------------------------------------
# Shared library version
#-------------------------------------------------------------------------------
set( XRD_EC_VERSION   1.0.0 )
set( XRD_EC_SOVERSION 1 )

#-------------------------------------------------------------------------------
# The XrdEc shared library
#-------------------------------------------------------------------------------
add_library(
  XrdEc
  SHARED
  XrdEcDataObject.hh           XrdEcDataObject.cc
  XrdEcConfig.hh
  XrdEcRedundancyProvider.hh   XrdEcRedundancyProvider.cc 
  XrdEcUtilities.hh            XrdEcUtilities.cc
  XrdEcRandomRead.hh           XrdEcRandomRead.cc
  XrdEcWriteBlock.hh           XrdEcWriteBlock.cc
  XrdEcReadBlock.hh            XrdEcReadBlock.cc
  XrdEcTruncate.hh             XrdEcTruncate.cc
  XrdEcScheduleHandler.hh      XrdEcScheduleHandler.cc
)

target_link_libraries(
  XrdEc
  XrdPosix
  XrdCl
  XrdUtils
  isal
)

set_target_properties(
  XrdEc
  PROPERTIES
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES ""
  VERSION   ${XRD_EC_VERSION}
  SOVERSION ${XRD_EC_SOVERSION} )
  
add_dependencies( XrdEc isa-l )


