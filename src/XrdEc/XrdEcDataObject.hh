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

#include "XrdEc/XrdEcEosAdaptor.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcLogger.hh"
#include "XrdEc/XrdEcObjCfg.hh"

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

      DataObject( const std::string &url, uint64_t size ) : url( url ), objsize( size ) // this should throw if EOS does not give as an object name TODO
      {
        MgmFeedback feedback = EosAdaptor::ResolveDummy( url, Config::Instance().headnode );
        objname   = std::move( feedback.objname );
        plgr      = std::move( feedback.plgr );
        signature = std::move( feedback.signature );

        // construct the object configuration
        objcfg.reset( new ObjCfg( objname ) );
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

        XrdCl::ResponseHandler *rdHandler = GetRdHandler( *objcfg, offset, size, buff, handler );

        while( size > 0 )
        {
          // if the user issued a read that is aligned with our block size
          // we can read directly into the user buffer
          if( BlockAligned( *objcfg, offset, size ) )
          {
            ReadBlock( *objcfg, signature, plgr, offset, buff, rdHandler );
            size   -= objcfg->datasize;
            offset += objcfg->datasize;
            buff   += objcfg->datasize;
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
              rdbuff.reset( new RdBuff( *objcfg, offset ) );
              // otherwise read out a new block into the read buffer
              // (the handler will copy the data into user buffer)
              ReadBlock( *objcfg, signature, plgr, rdbuff, rdHandler );
              uint32_t nbrd = objcfg->datasize;
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

        uint64_t firstblk  = offset / objcfg->datasize;
        uint64_t blkoff    = offset - firstblk * objcfg->datasize;
        uint32_t nbrd      = 0;
        uint32_t left      = size;
        RandRdHandler *rdHandler = new RandRdHandler( *objcfg, offset, size, buff, handler );

        for( uint64_t blknb = firstblk; nbrd < size; ++blknb )
        {
          uint32_t blkrdsize = left;
          if( blkrdsize > objcfg->datasize - blkoff )
            blkrdsize = objcfg->datasize - blkoff;
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
        if( !wrtbuff ) wrtbuff.reset( new WrtBuff( *objcfg, offset, mode ) );

        while( wrtsize > 0 )
        {
          uint64_t written = wrtbuff->Write( offset, wrtsize, buff, handler );
          offset  += written;
          buff    += written;
          wrtsize -= written;

          if( wrtbuff->Complete() )
          {
            wrthandler.IncCnt();
            WriteBlock( *objcfg, signature, plgr, std::move( *wrtbuff ),  &wrthandler );
            WrtMode  mode = offset < objsize ? WrtMode::Overwrite : WrtMode::New;
            wrtbuff.reset( new WrtBuff( *objcfg, offset, mode ) );
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
          WriteBlock( *objcfg, signature, plgr, std::move( *wrtbuff ), &wrthandler );
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
          uint64_t lastblk  = size - ( size % objcfg->datasize );
          uint32_t lastsize = size - lastblk;
          TruncateHandler* hndlr = new TruncateHandler( *objcfg, objsize, size, handler );

          if( lastsize > 0 )
          {
            TruncateBlock( *objcfg, signature, plgr, lastblk / objcfg->datasize, lastsize, hndlr );
            lastblk += objcfg->datasize;
          }

          for( uint64_t blk = lastblk; blk < objsize; blk += objcfg->datasize )
            RemoveBlock( *objcfg, signature, plgr, blk / objcfg->datasize, hndlr );

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
        uint64_t lastblk = offset - ( offset % objcfg->datasize );

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
          objsize = wrtbuff->GetOffset() + objcfg->datasize;
          wrtbuff->Pad( objcfg->datasize - wrtbuff->GetBlkSize() );
          // we need to flush the current write-buffer
          Flush();
        }
        wrtbuff.reset( new WrtBuff( *objcfg, offset, WrtMode::New ) );

        for( uint64_t blk = objsize; blk < lastblk; blk += objcfg->datasize )
        {
          wrthandler.IncCnt();
          CreateEmptyBlock( objname, signature, plgr, blk / objcfg->datasize, &wrthandler );
        }

        // if the new data are not aligned with the block size we need to pad them
        if( lastblk != offset ) wrtbuff->Pad( offset - lastblk );
      }

      XrdCl::URL               url;
      uint64_t                 objsize;
      std::string              objname;
      std::string              signature;
      std::unique_ptr<ObjCfg>  objcfg;

      placement_group          plgr;

      std::unique_ptr<WrtBuff> wrtbuff;
      StrmWrtHandler           wrthandler;
      std::shared_ptr<RdBuff>  rdbuff;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECDATAOBJECT_HH_ */
