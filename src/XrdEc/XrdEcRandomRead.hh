/*
 * XrdEcRandomRead.hh
 *
 *  Created on: Nov 7, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECRANDOMREAD_HH_
#define SRC_XRDEC_XRDECRANDOMREAD_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <string>
#include <mutex>

namespace XrdEc
{
  class RandRdHandler : public XrdCl::ResponseHandler
  {
    public:

      RandRdHandler( const ObjCfg           &objcfg,
                     uint64_t                offset,
                     uint32_t                size,
                     char                   *buffer,
                     XrdCl::ResponseHandler *handler ) : objcfg( objcfg ),
                                                         offset( offset ),
                                                         buffer( buffer ),
                                                         handler( handler ),
                                                         bytesrd( 0 ),
                                                         count( 0 )
      {
        uint64_t firstblk  = offset / objcfg.datasize;
        uint64_t blkoff    = offset - firstblk * objcfg.datasize;
        uint32_t nbrd      = objcfg.datasize - blkoff;
        if( nbrd ) ++count;
        if( size > nbrd ) size -= nbrd;
        else size = 0;
        count += size / objcfg.datasize;
        if( size % objcfg.datasize ) ++count;
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

      ObjCfg                  objcfg;
      uint64_t                offset;
      char                   *buffer;
      XrdCl::ResponseHandler *handler;
      uint32_t                bytesrd;
      uint32_t                count;
      std::mutex              mtx;
  };

  void ReadFromBlock( const ObjCfg            &objcfg,
                      const std::string       &sign,
                      const placement_group   &plgr,
                      uint64_t                 offset,
                      uint32_t                 size,
                      char                    *buffer,
                      XrdCl::ResponseHandler  *handler );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECRANDOMREAD_HH_ */
