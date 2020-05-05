/*
 * XrdEcClientPlugInFactory.hh
 *
 *  Created on: Nov 29, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECCLIENTPLUGINFACTORY_HH_
#define SRC_XRDEC_XRDECCLIENTPLUGINFACTORY_HH_

#include <XrdCl/XrdClPlugInInterface.hh>

extern "C"
{
  void *XrdClGetPlugIn( const void* /*arg*/ );
}

namespace XrdEc
{

  class ClientPlugInFactory : public XrdCl::PlugInFactory
  {
    public:

      ClientPlugInFactory();

      virtual ~ClientPlugInFactory();

      virtual XrdCl::FilePlugIn *CreateFile( const std::string &url );

      virtual XrdCl::FileSystemPlugIn *CreateFileSystem( const std::string &url );
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECCLIENTPLUGINFACTORY_HH_ */
