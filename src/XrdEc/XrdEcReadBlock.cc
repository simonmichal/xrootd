/*
 * XrdEcStreamRead.cc
 *
 *  Created on: Oct 18, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcReadBlock.hh"
#include "XrdEc/XrdEcRdBuff.hh"

#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

namespace XrdEc
{
  struct StrmRdCtx
  {
      StrmRdCtx( std::shared_ptr<RdBuff> &rdbuff, uint8_t nburl, XrdCl::ResponseHandler *handler ) :
                   nbnotfound( 0 ), nbok( 0 ), nberr( 0 ), nburls( nburl ), rdbuff( rdbuff ), bytesRead( 0 ), userHandler( handler )
      {
        XrdEc::Config &cfg = XrdEc::Config::Instance();

        memset( rdbuff->buffer, 0, cfg.datasize );
        paritybuff.reset( new char[cfg.paritysize] );
        memset( paritybuff.get(), 0, cfg.paritysize );

        stripes.reserve( cfg.nbchunks );
        for( uint8_t i = 0; i < cfg.nbdata; ++i )
          stripes.emplace_back( rdbuff->buffer + i * cfg.chunksize, false );
        for( uint8_t i = 0; i < cfg.nbparity; ++i )
          stripes.emplace_back( paritybuff.get() + i * cfg.chunksize, false );
      }

      void NotFound()
      {
        std::unique_lock<std::mutex> lck( mtx );
        ++nbnotfound;

        XrdEc::Config &cfg = XrdEc::Config::Instance();
        if( nbnotfound > nburls - cfg.nbchunks ) OnError();
      }

      void OnError()
      {
        std::unique_lock<std::mutex> lck( mtx );
        ++nberr;

        XrdEc::Config &cfg = XrdEc::Config::Instance();
        if( nberr > cfg.paritysize )
        {
          XrdCl::ResponseHandler *handler = userHandler;
          userHandler = 0;
          lck.unlock();
          handler->HandleResponse( new XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ), 0 );
        }
      }

      void OnSuccess( char *buffer, uint32_t size, uint8_t strpnb )
      {
        std::unique_lock<std::mutex> lck( mtx );
        ++nbok;
        bytesRead += size;

        // if there's no handler it means we already reconstructed this chunk
        // and called user handler!
        if( !userHandler ) return;

        // copy to the user buffer
        memcpy( stripes[strpnb].buffer, buffer, size );
        stripes[strpnb].valid = true;

        Config &cfg = Config::Instance();
        if( nbok == cfg.nbdata )
        {
          // check if there are missing data chunks
          for( uint8_t i = 0; i < cfg.nbdata; ++i )
            if( !( stripes[i].valid ) )
            {
              // there is at least one missing data chunk
              // so we need to recompute the missing ones
              cfg.redundancy.compute( stripes );
              break;
            }

          if( userHandler )
          {
            XrdCl::ResponseHandler *handler = userHandler;
            userHandler = 0;
            lck.unlock();
            XrdCl::ChunkInfo *chunk = new XrdCl::ChunkInfo();
            chunk->buffer = rdbuff->buffer;
            chunk->offset = rdbuff->offset;
            chunk->length = bytesRead;
            XrdCl::AnyObject *resp = new XrdCl::AnyObject();
            resp->Set( resp );
            handler->HandleResponse( new XrdCl::XRootDStatus(), resp );
          }
        }
      }

      uint8_t                              nbnotfound;
      uint8_t                              nbok;
      uint8_t                              nberr;
      uint8_t                              nburls;

      std::shared_ptr<RdBuff>              rdbuff;
      uint32_t                             bytesRead;

      std::unique_ptr<char[]>              paritybuff;

      stripes_t                            stripes;

      XrdCl::ResponseHandler*              userHandler;
      std::mutex                           mtx;
  };
}

namespace
{
  using namespace XrdEc;

  struct StrpData
  {
      StrpData( std::shared_ptr<StrmRdCtx> &ctx ) : notfound( false ), size( 0 ), strpnb( 0 ), ctx( ctx )
      {
        XrdEc::Config &cfg = XrdEc::Config::Instance();
        buffer.reset( new char[cfg.chunksize] );
        memset( buffer.get(), 0, XrdEc::Config::Instance().chunksize );
      }

      void Returned( bool ok )
      {
        if( notfound )
        {
          ctx->NotFound();
          return;
        }

        if( !ok || !ValidateChecksum() )
        {
          ctx->OnError();
          return;
        }

        ctx->OnSuccess( buffer.get(), size, strpnb );
      }

      bool ValidateChecksum()
      {
        // TODO
        return true;
      }

      bool                       notfound;
      std::unique_ptr<char[]>    buffer;
      uint32_t                   size;
      std::string                checksum;
      uint8_t                    strpnb;
      std::shared_ptr<StrmRdCtx> ctx;
  };

  static void ReadStripe( const std::string          &url,
                          std::shared_ptr<StrmRdCtx> &ctx )
  {
    using namespace XrdCl;

    Config &cfg = Config::Instance();
    std::unique_lock<std::mutex> lck( ctx->mtx );
    std::shared_ptr<File> file( new File() );
    std::shared_ptr<StrpData> strpdata( new StrpData( ctx ) );

    // Construct the pipeline
    Pipeline rdstrp = Open( file.get(), url, OpenFlags::Read ) >> [strpdata]( XRootDStatus &st ){ strpdata->notfound = ( !st.IsOK() && ( st.code == errNotFound ) ); }
                    | Parallel( Read( file.get(), 0, cfg.chunksize, strpdata->buffer.get() ) >> [strpdata]( XRootDStatus &st, ChunkInfo &chunk ){ if( st.IsOK() ) strpdata->size = chunk.length; },
                                GetXAttr( file.get(), "xrdec.checksum" ) >> [strpdata]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpdata->checksum = value; },
                                GetXAttr( file.get(), "xrdec.strpnb")    >> [strpdata]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpdata->strpnb = std::stoi( value ); }
                              ) >> [strpdata]( XRootDStatus &st ){ strpdata->Returned( st.IsOK() ); }
                    | Close( file.get() ) >> [file]( XRootDStatus &st ){ /*just making sure file is deallocated*/ };

    // Run the pipeline
    Async( std::move( rdstrp ) );
  }
}

namespace XrdEc
{
  XrdCl::ResponseHandler* GetRdHandler( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler )
  {
    Config &cfg = Config::Instance();

    // check if the read size and offset are aligned exactly
    // with a block, if yes we don't need a special handler
    if( !( offset % cfg.datasize ) && size == cfg.datasize )
      return handler;

    return 0; // TODO
  }

  bool BlockAligned( uint64_t offset, uint32_t size )
  {
    Config &cfg = Config::Instance();
    return !( offset % cfg.datasize ) && size >= cfg.datasize;
  }

  void ReadBlock( const std::string      &obj,
                  const std::string      &sign,
                  const placement_group  &plgr,
                  uint64_t                offset,
                  char                   *buffer,
                  XrdCl::ResponseHandler *handler )
  {
    std::shared_ptr<RdBuff> rdbuff( new RdBuff( offset, buffer ) );
    ReadBlock( obj, sign, plgr, rdbuff, handler );
  }

  void ReadBlock( const std::string       &obj,
                  const std::string       &sign,
                  const placement_group   &plgr,
                  std::shared_ptr<RdBuff> &rdbuff,
                  XrdCl::ResponseHandler  *handler )
  {
    Config &cfg = Config::Instance();
    std::shared_ptr<StrmRdCtx> ctx( new StrmRdCtx( rdbuff, plgr.size(), handler ) );

    // we don't know where the chunks are so we are trying all possible locations
    for( uint8_t i = 0; i < plgr.size(); ++i )
    {
      std::string url = std::get<0>( plgr[i] ) + '/' + obj + '.' + std::to_string( rdbuff->offset / cfg.datasize ) + '?' + "ost.sig=" + sign;
      ReadStripe( url, ctx );
    }
  }

} /* namespace XrdEc */
