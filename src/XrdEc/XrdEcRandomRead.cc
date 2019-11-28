/*
 * XrdEcRandomRead.cc
 *
 *  Created on: Nov 7, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcRandomRead.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdEc/XrdEcReadBlock.hh"
#include "XrdEc/XrdEcLogger.hh"

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

        std::stringstream ss;
        ss << "RandRdCtx::Handle (" << (void*)this << ") : count = " << count;
        XrdEc::Logger &log = XrdEc::Logger::Instance();
        log.Entry( ss.str() );

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
      StrpRdCtx( const XrdEc::ObjCfg        &objcfg,
                 std::shared_ptr<RandRdCtx> &ctx,
                 uint8_t                     strpnb,
                 uint64_t                    offset,
                 uint32_t                    size,
                 char                       *buffer ) : objcfg( objcfg ),
                                                        ctx( ctx ),
                                                        rdbuff( offset, size, buffer, objcfg.chunksize ),
                                                        blksize( 0 ),
                                                        strpnb( strpnb ),
                                                        chnb( 0 )
      {
      }

      void Handle( const XrdCl::XRootDStatus &st )
      {
        std::stringstream ss;
        ss << "StrpRdCtx::Handle (" << (void*)this << ") : url = " << url << ", st = " << ( st.IsOK() ? "OK" : "FAILED" ) << ", strpnb = " << strpnb << ", chnb = " << chnb;
        XrdEc::Logger &log = XrdEc::Logger::Instance();
        log.Entry( ss.str() );

        if( !st.IsOK() )
        {
          ctx->Handle( st );
          return;
        }

        if( strpnb != chnb )
        {
          ctx->Handle( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errInternal ) );
          return;
        }

        std::string calcchksum = XrdEc::CalcChecksum( (char*)rdbuff.buffer, objcfg.chunksize );
        if( checksum != calcchksum )
        {
          ctx->Handle( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ) );
          return;
        }

        uint64_t choff  = chnb * objcfg.chunksize;
        uint64_t chsize = blksize <= choff ? 0 : blksize - choff;
        if( chsize > objcfg.chunksize ) chsize = objcfg.chunksize;
        rdbuff.finalize( chsize );

        ctx->Handle( XrdCl::XRootDStatus() );
      }

      XrdEc::ObjCfg              objcfg;
      std::shared_ptr<RandRdCtx> ctx;
      read_buffer                rdbuff;
      std::string                checksum;
      uint64_t                   blksize;
      uint8_t                    strpnb;
      uint8_t                    chnb;
      std::string                url;
  };


  static void ReadStripe( const XrdEc::ObjCfg        &objcfg,
                          const std::string          &url,
                          uint8_t                     strpnb,
                          uint64_t                    offset,
                          uint32_t                    size,
                          char                       *buffer,
                          std::shared_ptr<RandRdCtx> &ctx )
  {
    using namespace XrdCl;

    std::shared_ptr<File>      file( new File() );
    std::shared_ptr<StrpRdCtx> strpctx( new StrpRdCtx( objcfg, ctx, strpnb, offset, size, buffer ) );
    strpctx->url = url;

    // Construct the pipeline
    Pipeline rdstrp = Open( file.get(), url, OpenFlags::Read )
                    | Parallel( Read( file.get(), 0, objcfg.chunksize, strpctx->rdbuff.buffer ),
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
      FallBackHandler( const XrdEc::ObjCfg    &objcfg,
                       uint64_t                offset,
                       uint32_t                size,
                       char                   *buffer,
                       XrdCl::ResponseHandler *handler ) : rdbuff( offset, size, buffer, objcfg.datasize ),
                                                           offset( offset ),
                                                           size( size ),
                                                           handler( handler )
      {
      }

      void HandleResponse( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response )
      {
        rdbuff.finalize();

        // if we were successful we might need to adjust the
        // read size on the response (otherwise we will always
        // report full block has been read)
        if( status->IsOK() )
        {
          XrdCl::ChunkInfo *chunk = 0;
          response->Get( chunk );
          if( offset + size > chunk->length )
            size = chunk->length - offset;
          chunk->length = size;
        }

        handler->HandleResponse( status, response );
        delete this;
      }

      read_buffer             rdbuff;
      uint64_t                offset;
      uint32_t                size;
      XrdCl::ResponseHandler *handler;
  };

  class RandRdHandlerPriv : public XrdCl::ResponseHandler
  {
    public:

      RandRdHandlerPriv( const XrdEc::ObjCfg          &objcfg,
                         const std::string            &sign,
                         const XrdEc::placement_group &plgr,
                         uint64_t                      offset,
                         uint32_t                      size,
                         char                         *buffer,
                         XrdCl::ResponseHandler       *handler ) : objcfg( objcfg ),
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

        uint64_t blkoff = offset % objcfg.datasize;
        uint32_t rdsize = size;
        if( blkoff + rdsize > objcfg.datasize )
          rdsize = objcfg.datasize - blkoff;

        FallBackHandler *fbHandler = new FallBackHandler( objcfg, blkoff, rdsize, buffer, handler );
        XrdEc::ReadBlock( objcfg, sign, plgr, offset, (char*)fbHandler->rdbuff.buffer, fbHandler );

        delete status;
        delete response;
        delete this;
      }

    private:

      XrdEc::ObjCfg                 objcfg;
      const std::string             sign;
      const XrdEc::placement_group  plgr;
      uint64_t                      offset;
      uint32_t                      size;
      char                         *buffer;
      XrdCl::ResponseHandler       *handler;
  };
}

namespace XrdEc
{
  void ReadFromBlock( const ObjCfg            &objcfg,
                      const std::string       &sign,
                      const placement_group   &plgr,
                      uint64_t                 offset,
                      uint32_t                 size,
                      char                    *buffer,
                      XrdCl::ResponseHandler  *handler )
  {
    uint64_t  blknb     = offset / objcfg.datasize;
    uint64_t  blkoff    = offset - blknb * objcfg.datasize;
    uint64_t  blkrdsize = size;
    if( blkrdsize > objcfg.datasize - blkoff ) blkrdsize = objcfg.datasize - blkoff;

    std::string blkname = objcfg.obj + '.' + std::to_string( blknb );
    placement_t placement = GeneratePlacement( objcfg, blkname, plgr, false );
    blkname += "?ost.sig=" + sign;

    handler = new RandRdHandlerPriv( objcfg, sign, plgr, offset, size, buffer, handler );
    std::shared_ptr<RandRdCtx> ctx( new RandRdCtx( offset, size, buffer, handler ) );
    uint64_t rdnb   = 0;
    uint8_t firstch = blkoff / objcfg.chunksize;
    uint64_t choff  = blkoff - firstch * objcfg.chunksize;

    uint8_t chnb = 0;
    for( chnb = firstch; rdnb < blkrdsize && chnb < objcfg.nbchunks; ++chnb )
    {
      std::string url = placement[chnb] + '/' + blkname;
      uint32_t chrdsize = blkrdsize - rdnb;
      if( chrdsize > objcfg.chunksize - choff ) chrdsize = objcfg.chunksize - choff;
      ReadStripe( objcfg, url, chnb, choff, chrdsize, buffer + rdnb, ctx );
      choff      = 0;
      rdnb      += chrdsize;
    }
  }
} /* namespace XrdEc */
