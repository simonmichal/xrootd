/*
 * XrdEcStreamRead.cc
 *
 *  Created on: Oct 18, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcReadBlock.hh"
#include "XrdEc/XrdEcRdBuff.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcLogger.hh"

#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

#include <mutex>
#include <sstream>

namespace XrdEc
{
  struct StrmRdCtx
  {
      StrmRdCtx( const ObjCfg &objcfg, std::shared_ptr<RdBuff> &rdbuff, uint8_t nburl, std::shared_ptr<CallbackWrapper> &callback ) :
                   objcfg( objcfg ), nbnotfound( 0 ), nbok( 0 ), nberr( 0 ), nburls( nburl ), rdbuff( rdbuff ), callback( callback ), done( false )
      {
        memset( rdbuff->buffer, 0, objcfg.datasize );
        paritybuff.reset( new char[objcfg.paritysize] );
        memset( paritybuff.get(), 0, objcfg.paritysize );

        stripes.reserve( objcfg.nbchunks );
        for( uint8_t i = 0; i < objcfg.nbdata; ++i )
          stripes.emplace_back( rdbuff->buffer + i * objcfg.chunksize, false );
        for( uint8_t i = 0; i < objcfg.nbparity; ++i )
          stripes.emplace_back( paritybuff.get() + i * objcfg.chunksize, false );
      }

      void NotFound()
      {
        std::unique_lock<std::mutex> lck( *callback );
        ++nbnotfound;

        std::stringstream ss;
        ss << "StrmRdCtx::NotFound (" << (void*) this << ") : nbnotfound = " << int( nbnotfound );
        Logger &log = Logger::Instance();
        log.Entry( ss.str() );

        // if there's no handler it means we already reconstructed this chunk
        // and called user handler! // TODO this is to be replaced with CallbackWrapper !!!
        if( done || !callback->Valid() )
        {
          lck.unlock();
          return;
        }

        if( nbnotfound + nberr > nburls - objcfg.nbdata )
        {
          // Run potential read callbacks!!!
          rdbuff->RunCallbacks( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ) );

          // we don't need to check if userHandler is valid
          // because we did this few lines before
          XrdCl::ResponseHandler *handler = callback->Release();
          done = true;
          lck.unlock();
          handler->HandleResponse( new XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ), 0 );
        }
      }

      void OnError()
      {
        std::unique_lock<std::mutex> lck( *callback );
        ++nberr;

        std::stringstream ss;
        ss << "StrmRdCtx::OnError (" << (void*) this << ") : nberr = " << int( nberr );
        Logger &log = Logger::Instance();
        log.Entry( ss.str() );

        // if there's no handler it means we already reconstructed this chunk
        // and called user handler! // TODO this is to be replaced with CallbackWrapper !!!
        if( done || !callback->Valid() )
        {
          lck.unlock();
          return;
        }

        if( nbnotfound + nberr > nburls - objcfg.nbdata  )
        {
          // Run potential read callbacks!!!
          rdbuff->RunCallbacks( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ) );

          // we don't need to check if userHandler is valid
          // because we did this few lines before
          XrdCl::ResponseHandler *handler = callback->Release();
          done = true;
          lck.unlock();
          handler->HandleResponse( new XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ), 0 );
      }
      }

      void OnSuccess( char *buffer, uint32_t size, uint8_t strpnb, uint32_t blksize )
      {
        std::unique_lock<std::mutex> lck( *callback );
        ++nbok;

        std::stringstream ss;
        ss << "StrmRdCtx::OnSuccess (" << (void*) this << ") : nbok = " << int( nbok );
        Logger &log = Logger::Instance();
        log.Entry( ss.str() );

        // if there's no handler it means we already reconstructed this chunk
        // and called user handler!
        if( done || !callback->Valid() )
        {
          lck.unlock();
          return;
        }

        // copy to the user buffer
        memcpy( stripes[strpnb].buffer, buffer, size );
        stripes[strpnb].valid = true;

        if( nbok == objcfg.nbdata )
        {
          // check if there are missing data chunks
          for( uint8_t i = 0; i < objcfg.nbdata; ++i )
            if( !( stripes[i].valid ) )
            {
              // there is at least one missing data chunk
              // so we need to recompute the missing ones
              Config &cfg = Config::Instance();
              cfg.GetRedundancy( objcfg ).compute( stripes ); // TODO it throws !!!
              break;
            }

          // Run potential read callbacks!!!
          rdbuff->RunCallbacks( XrdCl::XRootDStatus() );

          // we don't need to check if userHandler is valid
          // because we did this few lines before
          done = true;
          lck.unlock();
          XrdCl::ChunkInfo *chunk = new XrdCl::ChunkInfo();
          chunk->buffer = rdbuff->buffer;
          chunk->offset = rdbuff->offset;
          chunk->length = blksize;
          XrdCl::AnyObject *resp = new XrdCl::AnyObject();
          resp->Set( chunk );
          XrdCl::ResponseHandler *handler = callback->Get();
          handler->HandleResponse( new XrdCl::XRootDStatus(), resp );
        }
      }

      ObjCfg                               objcfg;
      uint8_t                              nbnotfound;
      uint8_t                              nbok;
      uint8_t                              nberr;
      uint8_t                              nburls;

      std::shared_ptr<RdBuff>              rdbuff;

      std::unique_ptr<char[]>              paritybuff;

      stripes_t                            stripes;

      std::shared_ptr<CallbackWrapper>     callback;

      bool                                 done;
  };
}

namespace
{
  using namespace XrdEc;

  struct StrpData
  {
      StrpData( const ObjCfg &objcfg, std::shared_ptr<StrmRdCtx> &ctx ) : objcfg( objcfg ), notfound( false ), size( 0 ), blksize( 0 ), strpnb( 0 ), ctx( ctx )
      {
        buffer.reset( new char[objcfg.chunksize] );
        memset( buffer.get(), 0, objcfg.chunksize );
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
        std::string checkSum = CalcChecksum( buffer.get(), objcfg.chunksize );
        return checkSum == checksum;
      }

      ObjCfg                     objcfg;
      bool                       notfound;
      std::unique_ptr<char[]>    buffer; // we cannot use RdBuff/user-buffer because we don't know which file contains witch chunk
      uint32_t                   size;
      std::string                checksum;
      uint64_t                   blksize;
      uint8_t                    strpnb;
      std::shared_ptr<StrmRdCtx> ctx;
  };

  static void ReadStripe( const ObjCfg               &objcfg,
                          const std::string          &url,
                          std::shared_ptr<StrmRdCtx> &ctx )
  {
    using namespace XrdCl;

    std::stringstream ss;
    ss << "ReadStripe (StrmRdCtx = " << ctx.get() << ") : url = " << url;
    Logger &log = Logger::Instance();
    log.Entry( ss.str() );

    std::unique_lock<std::mutex> lck( *ctx->callback );
    std::shared_ptr<File> file( new File( false ) );
    std::shared_ptr<StrpData> strpdata( new StrpData( objcfg, ctx ) );

    // Construct the pipeline
    Pipeline rdstrp = Open( file.get(), url, OpenFlags::Read ) >> [strpdata]( XRootDStatus &st ){ strpdata->notfound = ( !st.IsOK() && ( st.code == errErrorResponse ) && ( st.errNo == kXR_NotFound ) ); }
                    | Parallel( Read( file.get(), 0, objcfg.chunksize, strpdata->buffer.get() ) >> [strpdata]( XRootDStatus &st, ChunkInfo &chunk ){ if( st.IsOK() ) strpdata->size = chunk.length; },
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
      AcrossBlkRdHandler( const ObjCfg &objcfg, uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler ) : objcfg( objcfg ), offset( offset ), size( size ), buffer( buffer ), nbrd( 0 ), nbreq( 0 ), handler( handler ), failed( false )
      {
        // Note: the number of requests this handler will need to handle equals to the
        //       number of blocks one needs to read in order to satisfy the read request.

        // even if it's NOP we will have to handle one response!
        if( size == 0 )
        {
          nbreq = 1;
          return;
        }

        // check if we are aligned with our block size
        if( offset % objcfg.datasize )
        {
          // count the first block we will be reading from
          ++nbreq;
          // the offset of the next block
          uint64_t nextblk = offset - ( offset % objcfg.datasize ) + objcfg.datasize;
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
          if( size > objcfg.datasize ) size -= objcfg.datasize;
          else size = 0;
        }
      }

      void HandleResponse( XrdCl::XRootDStatus *st, XrdCl::AnyObject *rsp )
      {
        std::unique_lock<std::mutex> lck( mtx );
        Logger &log = Logger::Instance();

        // decrement the request counter
        --nbreq;

        std::stringstream ss;
        ss << "AcrossBlkRdHandler::HandleResponse (" << (void*) this << ") : nbreq = " << nbreq;
        log.Entry( ss.str() );

        if( !st->IsOK() || failed )
        {
          std::stringstream ss;
          ss << "AcrossBlkRdHandler::HandleResponse (" << (void*) this << ") : failed!";
          log.Entry( ss.str() );

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
          if( nbreq == 0 )
          {
            lck.unlock();
            delete this;
          }
          return;
        }

        // Do we need to copy the data to user buffer?
        // If the read was not aligned with block size
        // we might need to copy some data from the first
        // and last block!!!
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
          memmove( buffer, (char*)chunk->buffer + rspoff, rspsz );
          nbrd += rspsz;
        }
        else if( chunk->offset + objcfg.datasize > offset + size) // we are always trying to read the full block
        {
          // the block ends after the actual read size,
          // hence it has to be the last block
          uint32_t buffoff = chunk->offset - offset;
          // space left in the buffer
          uint32_t buffsz = size - buffoff;
          if( buffsz > chunk->length ) buffsz = chunk->length;
          if( buffer + buffoff != chunk->buffer ) // make sure the data were not copied to the destination buffer already
            memmove( buffer + buffoff, chunk->buffer, buffsz );
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
          lck.unlock();
          delete this;
        }

        delete st;
        delete rsp;
      }

    private:

      ObjCfg                  objcfg;
      uint64_t                offset;
      uint32_t                size;
      char                   *buffer;
      std::mutex              mtx;
      uint32_t                nbrd;  // number of bytes read
      uint32_t                nbreq; // number of requests this handler will need to handle
      XrdCl::ResponseHandler *handler;
      bool                    failed;
  };

  std::shared_ptr<CallbackWrapper> GetRdHandler( const ObjCfg &objcfg, uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler )
  {
    // check if the read size and offset are aligned exactly
    // with a block, if yes we don't need a special handler
    if( !( offset % objcfg.datasize ) && size == objcfg.datasize )
      return std::make_shared<CallbackWrapper>( handler );

    return std::make_shared<CallbackWrapper>( new AcrossBlkRdHandler( objcfg, offset, size, buffer, handler ) );
  }

  bool BlockAligned( const ObjCfg &objcfg, uint64_t offset, uint32_t size )
  {
    return !( offset % objcfg.datasize ) && size >= objcfg.datasize;
  }

  void ReadBlock( const ObjCfg                     &objcfg,
                  const std::string                &sign,
                  const placement_group            &plgr,
                  uint64_t                         offset,
                  char                             *buffer,
                  std::shared_ptr<CallbackWrapper> &callback )
  {
    std::shared_ptr<RdBuff> rdbuff( new RdBuff( objcfg, offset, buffer ) );
    ReadBlock( objcfg, sign, plgr, rdbuff, callback );
  }

  void ReadBlock( const ObjCfg                     &objcfg,
                  const std::string                &sign,
                  const placement_group            &plgr,
                  std::shared_ptr<RdBuff>          &rdbuff,
                  std::shared_ptr<CallbackWrapper> &callback )
  {
    std::shared_ptr<StrmRdCtx> ctx( new StrmRdCtx( objcfg, rdbuff, plgr.size(), callback ) );

    // we don't know where the chunks are so we are trying all possible locations
    for( uint8_t i = 0; i < plgr.size(); ++i )
    {
      std::string url = std::get<0>( plgr[i] ) + '/' + objcfg.obj + '.' + std::to_string( rdbuff->offset / objcfg.datasize ) + '?' + "ost.sig=" + sign;
      ReadStripe( objcfg, url, ctx );
    }
  }

} /* namespace XrdEc */
