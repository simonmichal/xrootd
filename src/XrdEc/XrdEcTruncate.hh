/*
 * XrdEcTruncate.hh
 *
 *  Created on: Nov 6, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECTRUNCATE_HH_
#define SRC_XRDEC_XRDECTRUNCATE_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <mutex>
#include <string>

namespace XrdEc
{
  class TruncateHandler : public XrdCl::ResponseHandler
  {
    public:

      TruncateHandler( uint64_t oldsize, uint64_t newsize, XrdCl::ResponseHandler *handler ) : handler( handler ), count( 0 )
      {
        Config &cfg = Config::Instance();
        uint64_t lastblk = newsize - ( newsize % cfg.datasize );
        for( uint64_t blk = lastblk; blk < oldsize; blk += cfg.datasize )
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

      XrdCl::ResponseHandler *handler;
      std::mutex              mtx;
      uint32_t                count;
  };

  void TruncateBlock( const std::string      &obj,
                      const std::string      &sign,
                      const placement_group  &plgr,
                      uint64_t                blknb,
                      uint64_t                size,
                      XrdCl::ResponseHandler *handler );

  void RemoveBlock( const std::string      &obj,
                    const std::string      &sign,
                    const placement_group  &plgr,
                    uint64_t                blknb,
                    XrdCl::ResponseHandler *handler );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECTRUNCATE_HH_ */
