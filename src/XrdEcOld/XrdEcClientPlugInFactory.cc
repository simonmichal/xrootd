/*
 * XrdEcClientPlugInFactory.cc
 *
 *  Created on: Nov 29, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcClientPlugInFactory.hh"
#include "XrdEc/XrdEcClientPlugIn.hh"
#include "XrdVersion.hh"


XrdVERSIONINFO(XrdClGetPlugIn, XrdClGetPlugIn)

extern "C"
{
  void *XrdClGetPlugIn( const void* /*arg*/ )
  {
    return static_cast<void*>( new XrdEc::ClientPlugInFactory());
  }
}

namespace XrdEc
{

  ClientPlugInFactory::ClientPlugInFactory()
  {
  }

  ClientPlugInFactory::~ClientPlugInFactory()
  {
  }

  XrdCl::FilePlugIn* ClientPlugInFactory::CreateFile( const std::string &url )
  {
    return new FilePlugIn();
  }

  XrdCl::FileSystemPlugIn* ClientPlugInFactory::CreateFileSystem( const std::string &url )
  {
    return new XrdCl::FileSystemPlugIn();
  }

} /* namespace XrdEc */
