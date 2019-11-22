/*
 * XrdEcStreamRead.cc
 *
 *  Created on: Oct 18, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcReadBlock.hh"
#include "XrdEc/XrdEcRdBuff.hh"
#include "XrdEc/XrdEcLogger.hh"

#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

#include <mutex>

namespace XrdEc
{
  struct StrmRdCtx
  {
      StrmRdCtx( std::shared_ptr<RdBuff> &rdbuff, uint8_t nburl, XrdCl::ResponseHandler *handler ) :
                   nbnotfound( 0 ), nbok( 0 ), nberr( 0 ), nburls( nburl ), rdbuff( rdbuff ), userHandler( handler )
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

          // Run potential read callbacks!!!
          rdbuff->RunCallbacks( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ) );
        }
      }

      void OnSuccess( char *buffer, uint32_t size, uint8_t strpnb, uint32_t blksize )
      {
        std::unique_lock<std::mutex> lck( mtx );
        ++nbok;

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
              cfg.redundancy.compute( stripes ); // TODO it throws !!!
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
            chunk->length = blksize;
            XrdCl::AnyObject *resp = new XrdCl::AnyObject();
            resp->Set( chunk );
            handler->HandleResponse( new XrdCl::XRootDStatus(), resp );
          }

          // Run potential read callbacks!!!
          rdbuff->RunCallbacks( XrdCl::XRootDStatus() );
        }
      }

      uint8_t                              nbnotfound;
      uint8_t                              nbok;
      uint8_t                              nberr;
      uint8_t                              nburls;

      std::shared_ptr<RdBuff>              rdbuff;

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
      StrpData( std::shared_ptr<StrmRdCtx> &ctx ) : notfound( false ), size( 0 ), blksize( 0 ), strpnb( 0 ), ctx( ctx )
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

        ctx->OnSuccess( buffer.get(), size, strpnb, blksize );
      }

      bool ValidateChecksum()
      {
        Config &cfg = Config::Instance();
        std::string checkSum = CalcChecksum( buffer.get(), cfg.chunksize );
        return checkSum == checksum;
      }

      bool                       notfound;
      std::unique_ptr<char[]>    buffer; // we cannot use RdBuff/user-buffer because we don't know which file contains witch chunk
      uint32_t                   size;
      std::string                checksum;
      uint64_t                   blksize;
      uint8_t                    strpnb;
      std::shared_ptr<StrmRdCtx> ctx;
  };

  static void ReadStripe( const std::string          &url,
                          std::shared_ptr<StrmRdCtx> &ctx )
  {
    using namespace XrdCl;

    Logger &log = Logger::Instance();
    std::string msg = __func__;
    msg            += " : " + url;
    log.Entry( std::move( msg ) );

    Config &cfg = Config::Instance();
    std::unique_lock<std::mutex> lck( ctx->mtx );
    std::shared_ptr<File> file( new File() );
    std::shared_ptr<StrpData> strpdata( new StrpData( ctx ) );

    // Construct the pipeline
    Pipeline rdstrp = Open( file.get(), url, OpenFlags::Read ) >> [strpdata]( XRootDStatus &st ){ strpdata->notfound = ( !st.IsOK() && ( st.code == errErrorResponse ) && ( st.errNo == kXR_NotFound ) ); }
                    | Parallel( Read( file.get(), 0, cfg.chunksize, strpdata->buffer.get() ) >> [strpdata]( XRootDStatus &st, ChunkInfo &chunk ){ if( st.IsOK() ) strpdata->size = chunk.length; },
                                GetXAttr( file.get(), "xrdec.checksum" ) >> [strpdata]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpdata->checksum = value; },
                                GetXAttr( file.get(), "xrdec.blksize" )  >> [strpdata]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpdata->blksize = std::stoull( value ); },
                                GetXAttr( file.get(), "xrdec.strpnb")    >> [strpdata]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) strpdata->strpnb = std::stoi( value ); }
                              ) >> [strpdata]( XRootDStatus &st ){ strpdata->Returned( st.IsOK() ); }
                    | Close( file.get() ) >> [file]( XRootDStatus &st ){ /*just making sure file is deallocated*/ };

    // Run the pipeline
    Async( std::move( rdstrp ) );
  }
}

namespace XrdEc
{
  class AcrossBlkRdHandler : public XrdCl::ResponseHandler
  {
    public:
      AcrossBlkRdHandler( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler ) : offset( offset ), size( size ), buffer( buffer), nbrd( 0 ), nbreq( 0 ), handler( handler ), failed( false )
      {
        Config &cfg = Config::Instance();

        // Note: the number of requests this handler will need to handle equals to the
        //       number of blocks one needs to read in order to satisfy the read request.

        // even if it's NOP we will have to handle one response!
        if( size == 0 )
        {
          nbreq = 1;
          return;
        }

        // check if we are aligned with our block size
        if( offset % cfg.datasize )
        {
          // count the first block we will be reading from
          ++nbreq;
          // the offset of the next block
          uint64_t nextblk = offset - ( offset % cfg.datasize ) + cfg.datasize;
          // maximum number of bytes we can read from the block
          // containing the initial offset
          uint32_t maxrdnb = nextblk - offset;
          // if the initial block contains all the data we are interested in
          // we are done
          if( maxrdnb >= size ) return;
          // let's account for the data we will read from the first block
          size -= maxrdnb;
        }

        while( size > 0 )
        {
          ++nbreq;
          if( size > cfg.datasize ) size -= cfg.datasize;
          else size = 0;
        }
      }

      void HandleResponse( XrdCl::XRootDStatus *st, XrdCl::AnyObject *rsp )
      {
        std::unique_lock<std::mutex> mtx;

        // decrement the request counter
        --nbreq;

        if( !st->IsOK() || failed )
        {
          // if this is the first failure call the user handler
          // and set the state to failed
          if( handler )
          {
            handler->HandleResponse( st, rsp );
            handler = 0;
            failed  = true;
          }
          // otherwise just delete the response, there's nothing
          // else to do as we already called the user handler
          else
          {
            delete st;
            delete rsp;
          }
          // if this is the last response delete itself
          if( nbreq == 0 ) delete this;
          return;
        }

        // Do we need to copy the data to user buffer?
        // If the read was not aligned with block size
        // we might need to copy some data from the first
        // and last block!!!

        Config &cfg = Config::Instance();

        XrdCl::ChunkInfo *chunk = 0;
        rsp->Get( chunk );

        // the block starts before the actual read offset,
        // hence it must be the first block
        if( chunk->offset < offset )
        {
          // offset in the response buffer
          uint32_t rspoff = offset - chunk->offset;
          // size of the response data
          uint32_t rspsz  = chunk->length - rspoff;
          if( rspsz > size ) rspsz = size;
          memcpy( buffer, (char*)chunk->buffer + rspoff, rspsz );
          nbrd += rspsz;
        }
        else if( chunk->offset + cfg.datasize > offset + size) // we are always trying to read the full block
        {
          // the block ends after the actual read size,
          // hence it has to be the last block
          uint32_t buffoff = chunk->offset - offset;
          // space left in the buffer
          uint32_t buffsz = size - buffoff;
          if( buffsz > chunk->length ) buffsz = chunk->length;
          memcpy( buffer + buffoff, chunk->buffer, buffsz );
          nbrd += buffsz;
        }
        else
          nbrd += chunk->length;

        if( nbreq == 0 )
        {
          if( handler )
          {
            XrdCl::ChunkInfo *chunk = new XrdCl::ChunkInfo();
            chunk->buffer = buffer;
            chunk->length = nbrd;
            chunk->offset = offset;
            XrdCl::AnyObject *response = new XrdCl::AnyObject();
            response->Set( chunk );
            handler->HandleResponse( new XrdCl::XRootDStatus(), response );
          }
          delete this;
        }

        delete st;
        delete rsp;
      }

    private:

      uint64_t                offset;
      uint32_t                size;
      char                   *buffer;
      uint32_t                nbrd;  // number of bytes read
      uint32_t                nbreq; // number of requests this handler will need to handle
      XrdCl::ResponseHandler *handler;
      bool                    failed;
  };

  XrdCl::ResponseHandler* GetRdHandler( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler )
  {
    Config &cfg = Config::Instance();

    // check if the read size and offset are aligned exactly
    // with a block, if yes we don't need a special handler
    if( !( offset % cfg.datasize ) && size == cfg.datasize )
      return handler;

    return new AcrossBlkRdHandler( offset, size, buffer, handler );
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
