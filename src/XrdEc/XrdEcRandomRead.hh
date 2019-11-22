/*
 * XrdEcRandomRead.hh
 *
 *  Created on: Nov 7, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECRANDOMREAD_HH_
#define SRC_XRDEC_XRDECRANDOMREAD_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <string>
#include <mutex>

namespace XrdEc
{
  class RandRdHandler : public XrdCl::ResponseHandler
  {
    public:

      RandRdHandler( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler ) : offset( offset ),
                                                                                                       buffer( buffer ),
                                                                                                       handler( handler ),
                                                                                                       bytesrd( 0 ),
                                                                                                       count( 0 )
      {
        Config &cfg = Config::Instance();
        uint64_t firstblk  = offset / cfg.datasize;
        uint64_t blkoff    = offset - firstblk * cfg.datasize;
        uint32_t nbrd      = cfg.datasize - blkoff;
        if( nbrd ) ++count;
        if( size > nbrd ) size -= nbrd;
        else size = 0;
        count += size / cfg.datasize;
        if( size % cfg.datasize ) ++count;
      }

      void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
      {
        std::unique_lock<std::mutex> lck( mtx );
        --count;

        if( !status->IsOK() )
        {
          if( handler )
          {
            handler->HandleResponse( status, response );
            handler = nullptr;
          }
          if( !count )
          {
            lck.unlock();
            delete this;
          }
          return;
        }

        XrdCl::ChunkInfo *chunk = nullptr;
        response->Get( chunk );
        bytesrd += chunk->length;

        delete status;
        delete response;

        if( !count )
        {
          if( handler )
          {
            XrdCl::ChunkInfo *chunk = new XrdCl::ChunkInfo();
            chunk->offset = offset;
            chunk->length = bytesrd;
            chunk->buffer = buffer;
            XrdCl::AnyObject *resp  = new XrdCl::AnyObject();
            resp->Set( chunk );
            handler->HandleResponse( new XrdCl::XRootDStatus(), resp );
          }
          lck.unlock();
          delete this;
        }
      }

    private:

      uint64_t                offset;
      char                   *buffer;
      XrdCl::ResponseHandler *handler;
      uint32_t                bytesrd;
      uint32_t                count;
      std::mutex              mtx;
  };

  void ReadFromBlock( const std::string       &obj,
                      const std::string       &sign,
                      const placement_group   &plgr,
                      uint64_t                 offset,
                      uint32_t                 size,
                      char                    *buffer,
                      XrdCl::ResponseHandler  *handler );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECRANDOMREAD_HH_ */
