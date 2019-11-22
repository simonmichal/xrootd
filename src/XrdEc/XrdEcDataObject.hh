/*
 * XrdEcStore.hh
 *
 *  Created on: Dec 3, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECDATAOBJECT_HH_
#define SRC_XRDEC_XRDECDATAOBJECT_HH_

#include "XrdEc/XrdEcReadBlock.hh"
#include "XrdEc/XrdEcRandomRead.hh"
#include "XrdEc/XrdEcWriteBlock.hh"
#include "XrdEc/XrdEcWrtBuff.hh"
#include "XrdEc/XrdEcRdBuff.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"
#include "XrdEc/XrdEcTruncate.hh"

#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcBlockPool.hh"
#include "XrdEc/XrdEcStripeReader.hh"

#include "XrdEc/XrdEcEosAdaptor.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcLogger.hh"

#include "XrdCl/XrdClXRootDResponses.hh"

#include <string>
#include <vector>
#include <memory>
#include <future>
#include <atomic>
#include <mutex>

#include <sstream>

namespace XrdEc
{

  class DataObject
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

            std::stringstream ss;
            ss << "StrmWrtHandler::HandleResponse (" << (void*)this << ") : wrtcnt = " << wrtcnt << ", st = " << ( st->IsOK() ? "OK" : "FAILED" );
            Logger &log = Logger::Instance();
            log.Entry( ss.str() );

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

      DataObject( const std::string &path, uint64_t size ) : path( path ), objsize( size )
      {
        MgmFeedback feedback = EosAdaptor::ResolveDummy( path, Config::Instance().headnode );
        objname   = std::move( feedback.objname );
        plgr      = std::move( feedback.plgr );
        signature = std::move( feedback.signature );
      }

      virtual ~DataObject()
      {
      }

      void StrmRead( uint64_t offset, uint32_t size, void *buffer, XrdCl::ResponseHandler *handler )
      {
        char *buff = reinterpret_cast<char*>( buffer );

        // check if we are not reading past the end of the file
        if( offset >= objsize )
        {
          ScheduleHandler( offset, 0, buff, handler );
          return;
        }

        // adjust the read size
        if( offset + size > objsize )
          size = objsize - offset;

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
            if( rdbuff && rdbuff->HasData( offset ))
            {
              // if the read buffer is not empty simply read the data from the buffer
              uint32_t nbrd = rdbuff->Read( offset, size, buff, rdHandler );
              size   -= nbrd;
              offset += nbrd;
              buff   += nbrd;
            }
            else
            {
              // create a new read buffer if it doesn't exist or if it doesn't contain
              // data that we are interested in
              rdbuff.reset( new RdBuff( offset ) );
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


      void RandomRead( uint64_t offset, uint32_t size, void *buffer, XrdCl::ResponseHandler *handler )
      {
        char *buff = reinterpret_cast<char*>( buffer );

        // check if we are not reading past the end of the file
        if( offset >= objsize )
        {
          ScheduleHandler( offset, 0, buff, handler );
          return;
        }

        // adjust the read size
        if( offset + size > objsize )
          size = objsize - offset;

        Config &cfg = Config::Instance();
        uint64_t firstblk  = offset / cfg.datasize;
        uint64_t blkoff    = offset - firstblk * cfg.datasize;
        uint32_t nbrd      = 0;
        uint32_t left      = size;
        RandRdHandler *rdHandler = new RandRdHandler( offset, size, buff, handler );

        for( uint64_t blknb = firstblk; nbrd < size; ++blknb )
        {
          uint32_t blkrdsize = left;
          if( blkrdsize > cfg.datasize - blkoff )
            blkrdsize = cfg.datasize - blkoff;
          ReadFromBlock( objname, signature, plgr, offset, blkrdsize, buff, rdHandler );
          nbrd   += blkrdsize;
          offset += blkrdsize;
          buff   += blkrdsize;
          left   -= blkrdsize;
          blkoff  = 0;
        }
      }


      void StrmWrite( uint64_t offset, uint32_t size, const void *buffer, XrdCl::ResponseHandler *handler )
      {
        XrdCl::XRootDStatus st = wrthandler.GetStatus();
        if( !st.IsOK() )
        {
          ScheduleHandler( handler, st );
          return;
        }

        if( offset > objsize ) HandleSparseWrite( offset );

        const char *buff = reinterpret_cast<const char*>( buffer );
        uint32_t wrtsize = size;
        WrtMode  mode    = offset < objsize ? WrtMode::Overwrite : WrtMode::New;
        if( !wrtbuff ) wrtbuff.reset( new WrtBuff( offset, mode ) );

        while( wrtsize > 0 )
        {
          uint64_t written = wrtbuff->Write( offset, wrtsize, buff, handler );
          offset  += written;
          buff    += written;
          wrtsize -= written;

          if( wrtbuff->Complete() )
          {
            wrthandler.IncCnt();
            WriteBlock( objname, signature, plgr, std::move( *wrtbuff ),  &wrthandler );
            WrtMode  mode = offset < objsize ? WrtMode::Overwrite : WrtMode::New;
            wrtbuff.reset( new WrtBuff( offset, mode ) );
          }
        }

        // Update object size (offset points now at the end of the current write request).
        if( offset > objsize )
          objsize = offset;
      }

      inline void Flush()
      {
        if( wrtbuff && !wrtbuff->Empty() )
        {
          wrtbuff->Encode();
          wrthandler.IncCnt();
          WriteBlock( objname, signature, plgr, std::move( *wrtbuff ), &wrthandler );
          wrtbuff.reset( 0 );
        }
      }

      inline void Sync( XrdCl::ResponseHandler *handler )
      {
        wrthandler.AddHandler( handler );
      }

      void Truncate( uint64_t size, XrdCl::ResponseHandler *handler )
      {
        if( size < objsize )
        {
          Config &cfg = Config::Instance();
          uint64_t lastblk  = size - ( size % cfg.datasize );
          uint32_t lastsize = size - lastblk;
          TruncateHandler* hndlr = new TruncateHandler( objsize, size, handler );

          if( lastsize > 0 )
          {
            TruncateBlock( objname, signature, plgr, lastblk / cfg.datasize, lastsize, hndlr );
            lastblk += cfg.datasize;
          }

          for( uint64_t blk = lastblk; blk < objsize; blk += cfg.datasize )
            RemoveBlock( objname, signature, plgr, blk / cfg.datasize, hndlr );

          objsize = size;
        }
        else if( size > objsize )
        {
          // TODO
          // growing the file
          ScheduleHandler( handler, XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errNotSupported ) );
        }
        else
          ScheduleHandler( handler );
      }

    private:

      void HandleSparseWrite( uint64_t offset )
      {
        Config &cfg = Config::Instance();
        uint64_t lastblk = offset - ( offset % cfg.datasize );

        // check if the data go to the current write-buffer
        if( wrtbuff && wrtbuff->GetOffset() == lastblk )
        {
          // if yes, simply add padding
          wrtbuff->Pad( offset - wrtbuff->GetOffset() - wrtbuff->GetBlkSize() );
          return;
        }

        // pad the buffered block
        if( wrtbuff && !wrtbuff->Empty() )
        {
          // update object size
          objsize = wrtbuff->GetOffset() + cfg.datasize;
          wrtbuff->Pad( cfg.datasize - wrtbuff->GetBlkSize() );
          // we need to flush the current write-buffer
          Flush();
        }
        wrtbuff.reset( new WrtBuff( offset, WrtMode::New ) );

        for( uint64_t blk = objsize; blk < lastblk; blk += cfg.datasize )
        {
          wrthandler.IncCnt();
          CreateEmptyBlock( objname, signature, plgr, blk / cfg.datasize, &wrthandler );
        }

        // if the new data are not aligned with the block size we need to pad them
        if( lastblk != offset ) wrtbuff->Pad( offset - lastblk );
      }

      std::string              path;
      uint64_t                 objsize;
      std::string              objname;
      std::string              signature;

      placement_group          plgr;

      std::unique_ptr<WrtBuff> wrtbuff;
      StrmWrtHandler           wrthandler;
      std::shared_ptr<RdBuff>  rdbuff;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECDATAOBJECT_HH_ */
