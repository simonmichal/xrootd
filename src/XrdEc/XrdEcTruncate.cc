/*
 * XrdEcTruncate.cc
 *
 *  Created on: Nov 6, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcTruncate.hh"
#include "XrdEc/XrdEcReadBlock.hh"
#include "XrdEc/XrdEcWriteBlock.hh"
#include "XrdEc/XrdEcWrtBuff.hh"

#include "XrdCl/XrdClFileSystemOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"
#include "XrdCl/XrdClFileSystem.hh"

#include <stdint.h>
#include <memory>
#include <mutex>

namespace XrdEc
{
  struct RmBlkCtx
  {
      RmBlkCtx( XrdCl::ResponseHandler *handler ) : handler( handler )
      {
      }

      ~RmBlkCtx()
      {
        XrdCl::ResponseHandler* hdlr = handler.exchange( nullptr );
        if( hdlr ) hdlr->HandleResponse( new XrdCl::XRootDStatus(), nullptr );
      }

      void Handle( const XrdCl::XRootDStatus &st )
      {
        if( !st.IsOK() && !( st.code == XrdCl::errErrorResponse && st.errNo == kXR_NotFound ) )
        {
          XrdCl::ResponseHandler* hdlr = handler.exchange( nullptr );
          if( hdlr ) hdlr->HandleResponse( new XrdCl::XRootDStatus( st ), nullptr );
        }
      }

      std::atomic<XrdCl::ResponseHandler*> handler;
  };


  void RemoveBlock( const std::string      &obj,
                    const std::string      &sign,
                    const placement_group  &plgr,
                    uint64_t                blknb,
                    XrdCl::ResponseHandler *handler )
  {
    using namespace XrdCl;

    std::string objname = obj + '.' + std::to_string( blknb ) + '?' + "ost.sig=" + sign;
    std::vector<XrdCl::Pipeline> rmblks;
    std::shared_ptr<RmBlkCtx> ctx( new RmBlkCtx( handler ) );

    for( auto &location : plgr )
    {
      URL url( std::get<0>( location ) + '/' + objname );
      std::shared_ptr<XrdCl::FileSystem> fs( new XrdCl::FileSystem( url ) );
      rmblks.emplace_back( Rm( *fs, url.GetPath() ) >> [fs, ctx]( XRootDStatus &st ){ ctx->Handle( st ); } );
    }

    Async( Parallel( rmblks ) );
  }

  class TruncateHandlerPriv : public XrdCl::ResponseHandler
  {
      enum BlkOperation
      {
        ReadBlk,
        RemoveBlk,
        WriteBlk
      };

    public:

      TruncateHandlerPriv( const std::string      &obj,
                           const std::string      &sign,
                           const placement_group  &plgr,
                           uint64_t                blknb,
                           uint64_t                size,
                           XrdCl::ResponseHandler *handler ) : blkop( ReadBlk ),
                                                               obj( obj ),
                                                               sign( sign ),
                                                               plgr( plgr ),
                                                               blknb( blknb ),
                                                               size( size ),
                                                               handler( handler )
      {
      }

      void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
      {
        if( !status->IsOK() )
        {
          handler->HandleResponse( status, response );
          delete this;
          return;
        }

        switch( blkop )
        {
          case ReadBlk:
          {
            XrdCl::ChunkInfo *chunk = nullptr;
            response->Get( chunk );
            buffer.reset( reinterpret_cast<char*>( chunk->buffer ) );
            blkop = RemoveBlk;
            RemoveBlock( obj, sign, plgr, blknb, this );
            break;
          }

          case RemoveBlk:
          {
            Config &cfg = Config::Instance();
            WrtBuff wrtbuff( blknb * cfg.datasize, WrtMode::New );
            wrtbuff.Write( blknb * cfg.datasize, size, buffer.get(), nullptr );
            blkop = WriteBlk;
            WriteBlock( obj, sign, plgr, std::move( wrtbuff ), this );
            break;
          }

          case WriteBlk:
          {
            handler->HandleResponse( new XrdCl::XRootDStatus(), nullptr );
            delete this;
            break;
          }

          default:
          {
            handler->HandleResponse( new XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errInternal ), nullptr );
            delete this;
            break;
          }
        }

        delete status;
        delete response;
      }

    private:

      BlkOperation            blkop;
      const std::string       obj;
      const std::string       sign;
      const placement_group   plgr;
      uint64_t                blknb;
      uint64_t                size;
      XrdCl::ResponseHandler *handler;
      std::unique_ptr<char[]> buffer;
  };

  void TruncateBlock( const std::string      &obj,
                      const std::string      &sign,
                      const placement_group  &plgr,
                      uint64_t                blknb,
                      uint64_t                size,
                      XrdCl::ResponseHandler *handler )
  {
    Config &cfg = Config::Instance();
    char *buffer = new char[cfg.datasize];
    ReadBlock( obj, sign, plgr, blknb * cfg.datasize, buffer, new TruncateHandlerPriv( obj, sign, plgr, blknb, size, handler ) );
  }

} /* namespace XrdEc */