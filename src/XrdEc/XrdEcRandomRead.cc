/*
 * XrdEcRandomRead.cc
 *
 *  Created on: Nov 7, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcRandomRead.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcReadBlock.hh"

#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

#include <string>
#include <memory>

namespace
{
  struct read_buffer
  {
      read_buffer( uint64_t offset, uint32_t size, char *buffer, uint32_t reqsize ) : buffer( nullptr ), offset( offset ), size( size ), usrbuff( buffer ), reqsize( reqsize )
      {
        if( offset != 0 || size != reqsize )
        {
          prvbuff.reset( new char[reqsize] );
          this->buffer = prvbuff.get();
        }
        else
          this->buffer = usrbuff;
        // zero the buffer in case we will be reading from a sparse file
        memset( this->buffer, 0, reqsize);
      }

      inline uint32_t finalize()
      {
        return finalize( reqsize );
      }

      inline uint32_t finalize( uint64_t realsize )
      {
        if( realsize <= offset) size = 0;
        else if( realsize < offset + size )
          size = realsize - offset;

        if( prvbuff )
        {
          memcpy( usrbuff, prvbuff.get() + offset, size );
          prvbuff.reset();
        }

        return size;
      }

      ~read_buffer()
      {
        finalize();
      }

      void *buffer;

    private:
      uint64_t                 offset;
      uint32_t                 size;
      char                    *usrbuff;
      std::unique_ptr<char[]>  prvbuff;
      uint32_t                 reqsize; // required size
  };

  struct RandRdCtx
  {
      RandRdCtx( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler  *handler ) : offset( offset ),
                                                                                                    size( size ),
                                                                                                    buffer( buffer ),
                                                                                                    handler( handler ),
                                                                                                    count( 0 )
      {
      }

      ~RandRdCtx()
      {
        if( handler )
        {
          XrdCl::AnyObject *resp = 0;
          if( status.IsOK() )
          {
            XrdCl::ChunkInfo *chunk = new XrdCl::ChunkInfo();
            chunk->offset = offset;
            chunk->length = size;
            chunk->buffer = buffer;
            resp = new XrdCl::AnyObject();
            resp->Set( chunk );
          }
          handler->HandleResponse( new XrdCl::XRootDStatus( status ), resp );
        }
      }

      void Handle( const XrdCl::XRootDStatus &st )
      {
        std::unique_lock<std::mutex> lck( mtx );
        ++count;
        if( status.IsOK() && !st.IsOK() )
          status = st;
      }

      uint64_t                offset;
      uint32_t                size;
      char                   *buffer;
      XrdCl::ResponseHandler *handler;
      std::mutex              mtx;
      XrdCl::XRootDStatus     status;
      int                     count;
  };

  struct StrpRdCtx
  {
      StrpRdCtx( std::shared_ptr<RandRdCtx> &ctx,
                 uint8_t                     strpnb,
                 uint64_t                    offset,
                 uint32_t                    size,
                 char                       *buffer ) : ctx( ctx ),
                                                        rdbuff( offset, size, buffer, XrdEc::Config::Instance().chunksize ),
                                                        blksize( 0 ),
                                                        strpnb( strpnb ),
                                                        chnb( 0 )
      {
      }

      void Handle( const XrdCl::XRootDStatus &st )
      {
        if( strpnb != chnb )
        {
          ctx->Handle( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errInternal ) );
          return;
        }

        XrdEc::Config &cfg = XrdEc::Config::Instance();
        std::string calcchksum = XrdEc::CalcChecksum( (char*)rdbuff.buffer, cfg.chunksize );
        if( checksum != calcchksum )
        {
          ctx->Handle( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ) );
          return;
        }

        uint64_t choff  = chnb * cfg.chunksize;
        uint64_t chsize = blksize <= choff ? 0 : blksize - choff;
        if( chsize > cfg.chunksize ) chsize = cfg.chunksize;
        rdbuff.finalize( chsize );
        ctx->Handle( XrdCl::XRootDStatus() );
      }

      std::shared_ptr<RandRdCtx> ctx;
      read_buffer                rdbuff;
      std::string                checksum;
      uint64_t                   blksize;
      uint8_t                    strpnb;
      uint8_t                    chnb;
      std::string                url;
  };


  static void ReadStripe( const std::string          &url,
                          uint8_t                     strpnb,
                          uint64_t                    offset,
                          uint32_t                    size,
                          char                       *buffer,
                          std::shared_ptr<RandRdCtx> &ctx )
  {
    using namespace XrdCl;

    std::shared_ptr<File>      file( new File() );
    std::shared_ptr<StrpRdCtx> strpctx( new StrpRdCtx( ctx, strpnb, offset, size, buffer ) );
    strpctx->url = url;

    // Construct the pipeline
    XrdEc::Config &cfg = XrdEc::Config::Instance();
    Pipeline rdstrp = Open( file.get(), url, OpenFlags::Read )
                    | Parallel( Read( file.get(), 0, cfg.chunksize, strpctx->rdbuff.buffer ),
                                GetXAttr( file.get(), "xrdec.checksum" ) >> [strpctx]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpctx->checksum = value; },
                                GetXAttr( file.get(), "xrdec.blksize" )  >> [strpctx]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpctx->blksize = std::stoull( value ); },
                                GetXAttr( file.get(), "xrdec.strpnb")    >> [strpctx]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpctx->chnb = std::stoi( value ); }
                              ) >> [strpctx]( XRootDStatus &st ){ strpctx->Handle( st ); }
                    | Close( file.get() ) >> [file, strpctx]( XRootDStatus &st ){ /*just making sure file is deallocated*/ };

    // Run the pipeline
    Async( std::move( rdstrp ) );

  }

  struct FallBackHandler : public XrdCl::ResponseHandler
  {
      FallBackHandler( uint64_t                offset,
                       uint32_t                size,
                       char                   *buffer,
                       XrdCl::ResponseHandler *handler ) : rdbuff( offset, size, buffer, XrdEc::Config::Instance().datasize ),
                                                           handler( handler )
      {
      }

      void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
      {
        rdbuff.finalize();
        handler->HandleResponse( status, response );
        delete this;

      }

      read_buffer             rdbuff;
      XrdCl::ResponseHandler *handler;
  };

  class RandRdHandlerPriv : public XrdCl::ResponseHandler
  {
    public:

      RandRdHandlerPriv( const std::string            &obj,
                     const std::string            &sign,
                     const XrdEc::placement_group &plgr,
                     uint64_t                      offset,
                     uint32_t                      size,
                     char                         *buffer,
                     XrdCl::ResponseHandler       *handler ) : obj( obj ),
                                                               sign( sign ),
                                                               plgr( plgr ),
                                                               offset( offset ),
                                                               size( size ),
                                                               buffer( buffer ),
                                                               handler( handler )
    {
    }

      void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
      {
        if( status->IsOK() )
        {
          handler->HandleResponse( status, response );
          delete this;
          return;
        }

        FallBackHandler *fbHandler = new FallBackHandler( offset, size, buffer, handler );
        XrdEc::ReadBlock( obj, sign, plgr, offset, (char*)fbHandler->rdbuff.buffer, fbHandler );

        delete status;
        delete response;
        delete this;
      }

    private:

      const std::string            &obj;
      const std::string            &sign;
      const XrdEc::placement_group &plgr;
      uint64_t                      offset;
      uint32_t                      size;
      char                         *buffer;
      XrdCl::ResponseHandler       *handler;
  };
}

namespace XrdEc
{
  void ReadFromBlock( const std::string       &obj,
                      const std::string       &sign,
                      const placement_group   &plgr,
                      uint64_t                 offset,
                      uint32_t                 size,
                      char                    *buffer,
                      XrdCl::ResponseHandler  *handler )
  {
    Config   &cfg = Config::Instance();
    uint64_t  blknb     = offset / cfg.datasize;
    uint64_t  blkoff    = offset - blknb * cfg.datasize;
    uint64_t  blkrdsize = size;
    if( blkrdsize > cfg.datasize - blkoff ) blkrdsize = cfg.datasize - blkoff;

    std::string objname = obj + '.' + std::to_string( blknb ) + '?' + "ost.sig=" + sign;
    placement_t placement = GeneratePlacement( objname, plgr, false );

    std::shared_ptr<RandRdCtx> ctx( new RandRdCtx( offset, size, buffer, handler ) );
    uint64_t rdnb   = 0;
    uint8_t firstch = blkoff / cfg.chunksize;
    uint64_t choff  = blkoff - firstch * cfg.chunksize;

    uint8_t chnb = 0;
    for( chnb = firstch; rdnb < blkrdsize && chnb < cfg.nbchunks; ++chnb )
    {
      std::string url = placement[chnb] + '/' + objname;
      uint32_t chrdsize = blkrdsize - rdnb;
      if( chrdsize > cfg.chunksize - choff ) chrdsize = cfg.chunksize - choff;
      ReadStripe( url, chnb, choff, chrdsize, buffer + rdnb, ctx );
      choff      = 0;
      rdnb      += chrdsize;
    }
  }
} /* namespace XrdEc */
