/*
 * XrdEcStore.hh
 *
 *  Created on: Dec 3, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECDATASTORE_HH_
#define SRC_XRDEC_XRDECDATASTORE_HH_

#include "XrdEc/XrdEcReadBlock.hh"
#include "XrdEc/XrdEcWriteBlock.hh"
#include "XrdEc/XrdEcWrtBuff.hh"
#include "XrdEc/XrdEcRdBuff.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"

#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcBlockPool.hh"
#include "XrdEc/XrdEcStripeReader.hh"

#include "XrdEc/XrdEcEosAdaptor.hh"
#include "XrdEc/XrdEcConfig.hh"

#include "XrdCl/XrdClXRootDResponses.hh"

#include <string>
#include <vector>
#include <memory>
#include <future>
#include <atomic>
#include <mutex>


namespace XrdEc
{

  class DataStore
  {
      class StrmWrtHandler : public XrdCl::ResponseHandler
      {
        public:

          StrmWrtHandler() : wrtcnt( 0 ), handler( 0 )
          {
          }

          void HandleResponse( XrdCl::XRootDStatus *st, XrdCl::AnyObject *rsp )
          {
            std::unique_lock<std::mutex> lck( mtx );
            --wrtcnt;

            if( !st->IsOK() && status.IsOK() )
              status = *st;
            delete st;

            if( wrtcnt == 0 && handler )
            {
              handler->HandleResponse( new XrdCl::XRootDStatus( status ), 0 );
              handler = 0;
            }
          }

          XrdCl::XRootDStatus GetStatus()
          {
            std::unique_lock<std::mutex> lck( mtx );
            return status;
          }

          void IncCnt()
          {
            std::unique_lock<std::mutex> lck( mtx );
            ++wrtcnt;
          }

          void AddHandler( XrdCl::ResponseHandler *hndlr )
          {
            std::unique_lock<std::mutex> lck( mtx );
            if( wrtcnt > 0 )
            {
              handler = hndlr;
              return;
            }

            ScheduleHandler( hndlr, status );
          }

        private:

          XrdCl::XRootDStatus     status;
          std::mutex              mtx;
          uint32_t                wrtcnt;
          XrdCl::ResponseHandler *handler;
      };

    public:

      DataStore( const std::string &path, bool force ) : path( path ), force( force ), generator( new std::default_random_engine( strhash( path ) ) )
      {

        MgmFeedback feedback = EosAdaptor::ResolveDummy( path, Config::Instance().headnode );
        objname   = std::move( feedback.objname );
        plgr      = std::move( feedback.plgr );
        signature = std::move( feedback.signature );

      }

      virtual ~DataStore()
      {
      }

      uint64_t Read( uint64_t offset, uint64_t size, void *buffer )
      {
        char *buff = reinterpret_cast<char*>( buffer );
        StripeReader reader( objname, plgr, *generator.get() );
        return reader.Read( offset, size, buff );
      }


      void Write( uint64_t offset, uint64_t size, const void *buffer )
      {
        if( !wrtcache )
        {
          wrtcache.reset( new Block( BlockPool::Create() ) );
          wrtcache->Reset( path, offset, plgr ); // TODO if we are going to overwrite the whole block there is no need to pre-load the old value
        }

        const char *buff = reinterpret_cast<const char*>( buffer );

        while( size > 0 )
        {
          size_t written = wrtcache->Write( offset, size, buff );

          offset += written;
          buff   += written;

          if( written < size )
            wrtcache->Sync();

          size -= written;
        }
      }

      void StrmRead( uint64_t offset, uint32_t size, void *buffer, XrdCl::ResponseHandler *handler )
      {
        char *buff = reinterpret_cast<char*>( buffer );
        XrdCl::ResponseHandler *rdHandler = GetRdHandler( offset, size, buff, handler );
        Config &cfg = Config::Instance();

        while( size > 0 )
        {
          // if the user issued a read that is aligned with our block size
          // we can read directly into the user buffer
          if( BlockAligned( offset, size ) )
          {
            ReadBlock( objname, signature, plgr, offset, buff, rdHandler );
            size   -= cfg.datasize;
            offset += cfg.datasize;
            buff   += cfg.datasize;
          }
          else
          {
            // create a new read buffer if it doesn't exist or if it doesn't contain
            // data that we are interested in
            if( !rdbuff || !rdbuff->HasData( offset ) ) rdbuff.reset( new RdBuff( offset ) );

            if( !rdbuff->Empty() )
            {
              // if the read buffer is not empty simply read the data from the buffer
              uint32_t nbrd = rdbuff->Read( offset, size, buff, rdHandler );
              size   -= nbrd;
              offset += nbrd;
              buff   += nbrd;
            }
            else
            {
              // otherwise read out a new block into the read buffer
              // (the handler will copy the data into user buffer)
              ReadBlock( objname, signature, plgr, rdbuff, rdHandler );
              uint32_t nbrd = cfg.datasize;
              if( nbrd > size ) nbrd = size;
              size   -= nbrd;
              offset += nbrd;
              buff   += nbrd;
            }
          }
        }
      }


      void StrmWrite( uint64_t offset, uint32_t size, const void *buffer, uint64_t finalSize, XrdCl::ResponseHandler *handler )
      {
        XrdCl::XRootDStatus st = wrthandler.GetStatus();
        if( !st.IsOK() )
        {
          ScheduleHandler( handler, st );
          return;
        }

        const char *buff = reinterpret_cast<const char*>( buffer );
        if( !wrtbuff ) wrtbuff.reset( new WrtBuff( offset ) );

        while( size > 0 )
        {
          uint64_t written = wrtbuff->Write( offset, size, buff, handler );
          offset += written;
          buff   += written;
          size   -= written;

          if( wrtbuff->Complete() )
          {
            wrthandler.IncCnt();
            WriteBlock( objname, signature, plgr, finalSize, std::move( *wrtbuff ), &wrthandler ); // TODO we need a handler here, not user handler but operation status handler !!!
            wrtbuff.reset( new WrtBuff( offset ) );
          }
        }
      }

      inline void Flush( uint64_t finalSize )
      {
        if( wrtbuff && !wrtbuff->Empty() )
        {
          wrthandler.IncCnt();
          wrtbuff->Encode();
          WriteBlock( objname, signature, plgr, finalSize, std::move( *wrtbuff ), &wrthandler );
          wrtbuff.reset( 0 );
        }
      }

      inline void Sync( XrdCl::ResponseHandler *handler )
      {
        wrthandler.AddHandler( handler );
      }

      void Truncate( uint64_t size ) // can be used to wipe out the data
      {
        // TODO
//        Config &cfg = Config::Instance();
//
//        uint64_t lstblk  = size % cfg.datasize;
//        uint64_t fullblk = size / cfg.datasize;
//        uint64_t nbblk   = fullblk + ( lstblk ? 1 : 0 );
//
//        if( nbblk > placement.size() )
//        {
//          TruncateBlock( placement.size() - 1, cfg.datasize );
//          AddEmptyBlocks( fullblk, lstblk );
//        }
//        else // if( nbblk <= placement.size() )
//        {
//          RemoveBlocks( nbblk );
//          if( !lstblk ) lstblk = cfg.datasize;
//          TruncateBlock( nbblk - 1, lstblk );
//        }
      }

      void Finalize();

    private:

      void RemoveBlocks( uint64_t nbblk );

      void TruncateBlock( uint64_t blkid, uint64_t blksize );

      void AddEmptyBlocks( uint64_t fullblk, uint64_t lstblk );

      std::string              path;
      std::string              objname;
      std::string              signature;
      bool                     force;

    public:
      placement_group          plgr;

    private:

      std::unique_ptr<WrtBuff> wrtbuff;
      StrmWrtHandler           wrthandler;
      std::shared_ptr<RdBuff>  rdbuff;

      std::unique_ptr<Block>   wrtcache;

      //------------------------------------------------------------------------
      //! Hash function - for seeding the random number generator
      //------------------------------------------------------------------------
      static std::hash<std::string>  strhash;

      //------------------------------------------------------------------------
      //! Random number generator (for the placement policy)
      //------------------------------------------------------------------------
      std::unique_ptr<std::default_random_engine>  generator;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECDATASTORE_HH_ */
