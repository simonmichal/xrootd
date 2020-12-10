/*
 * XrdFecStripeSyncHandler.hh
 *
 *  Created on: Jul 12, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_XRDFECSTRIPESYNCHANDLER_HH_
#define SRC_XRDFEC_XRDFECSTRIPESYNCHANDLER_HH_

#include <vector>
#include <string>
#include <memory>

#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdSys/XrdSysPthread.hh"


class StripeSyncHandler : public XrdCl::ResponseHandler
{
  public:

    StripeSyncHandler( size_t size );

    virtual void HandleResponseWithHosts( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response, XrdCl::HostList *hostList );

    void Release();

    void Wait();

  private:
    XrdSysCondVar   mCond;
    size_t          mCounter;
};

#endif /* SRC_XRDFEC_XRDFECSTRIPESYNCHANDLER_HH_ */
