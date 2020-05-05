/*
 * XrdEcTruncate.hh
 *
 *  Created on: Nov 6, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECTRUNCATE_HH_
#define SRC_XRDEC_XRDECTRUNCATE_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <mutex>
#include <string>

namespace XrdEc
{
  class TruncateHandler : public XrdCl::ResponseHandler
  {
    public:

      TruncateHandler( const ObjCfg &objcfg, uint64_t oldsize, uint64_t newsize, XrdCl::ResponseHandler *handler ) : objcfg( objcfg ), handler( handler ), count( 0 )
      {
        uint64_t lastblk = newsize - ( newsize % objcfg.datasize );
        for( uint64_t blk = lastblk; blk < oldsize; blk += objcfg.datasize )
          ++count;
      }

      virtual void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject* )
      {
        std::unique_lock<std::mutex> lck( mtx );
        --count;

        XrdCl::XRootDStatus st = *status;
        delete status;

        if( !st.IsOK() && handler )
        {
          handler->HandleResponse( new XrdCl::XRootDStatus( st ), nullptr );
          handler = nullptr;
        }

        if( !count )
        {
          if( handler )
            handler->HandleResponse( new XrdCl::XRootDStatus(), nullptr );
          lck.unlock();
          delete this;
        }
      }

    private:

      ObjCfg                  objcfg;
      XrdCl::ResponseHandler *handler;
      std::mutex              mtx;
      uint32_t                count;
  };

  void TruncateBlock( const ObjCfg           &objcfg,
                      const std::string      &sign,
                      const placement_group  &plgr,
                      uint64_t                blknb,
                      uint64_t                size,
                      XrdCl::ResponseHandler *handler );

  void RemoveBlock( const ObjCfg           &objcfg,
                    const std::string      &sign,
                    const placement_group  &plgr,
                    uint64_t                blknb,
                    XrdCl::ResponseHandler *handler );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECTRUNCATE_HH_ */
