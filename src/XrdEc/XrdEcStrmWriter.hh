/*
 * XrdEcStrmWriter.hh
 *
 *  Created on: 5 May 2020
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECSTRMWRITER_HH_
#define SRC_XRDEC_XRDECSTRMWRITER_HH_

#include "XrdEc/XrdEcObjInterfaces.hh"
#include "XrdEc/XrdEcWrtBuff.hh"
#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

#include <future>
#include <atomic>
#include <random>
#include <chrono>

namespace XrdEc
{

  class StrmWriter : public ObjOperation
  {
    public:

      StrmWriter( const ObjCfg &objcfg ) : ObjOperation( objcfg ),
                                           wrt_callback( new WrtCallback() ),
                                           random_engine( std::chrono::system_clock::now().time_since_epoch().count() )
      {
        size_t size = objcfg.plgr.size();
        offsets.reset( new std::atomic<uint32_t>[size] );
        std::fill( offsets.get(), offsets.get() + size, 0 );
        dirs.reserve( size );
        for( size_t i = 0; i < size; ++i )
          dirs.emplace_back( &objcfg );
      }

      virtual ~StrmWriter()
      {
      }

      void Open( XrdCl::ResponseHandler *handler )
      {
        const size_t size = objcfg->plgr.size();

        std::vector<XrdCl::Pipeline> opens;
        opens.reserve( size );
        files.resize( size );

        for( size_t i = 0; i < size; ++i )
        {
          std::string url = objcfg->plgr[i] + objcfg->obj + ".zip";
          opens.emplace_back( XrdCl::Open( files[i], url, XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write ) );
        }

        XrdCl::Async( XrdCl::Parallel( opens ) >> handler ); // TODO Not all need to succeed !!!
      }

      void Write( uint64_t offset, uint32_t size, const void *buffer, XrdCl::ResponseHandler *handler )
      {
        const XrdCl::XRootDStatus &wrt_status = wrt_callback->GetWrtStatus();
        if( !wrt_status.IsOK() )
        {
          ScheduleHandler( handler, wrt_status );
          return;
        }

        const char* buff = reinterpret_cast<const char*>( buffer );

        uint32_t wrtsize = size;
        while( wrtsize > 0 )
        {
          if( !wrtbuff ) wrtbuff.reset( new WrtBuff( *objcfg, offset ) );
          uint64_t written = wrtbuff->Write( offset, wrtsize, buff, handler );
          offset  += written;
          buff    += written;
          wrtsize -= written;
          if( wrtbuff->Complete() ) WriteBlock(); //< run this in separate thread
        }
      }

      void Close( XrdCl::ResponseHandler *handler )
      {
        const XrdCl::XRootDStatus &wrt_status = wrt_callback->GetWrtStatus();
        if( !wrt_status.IsOK() )
        {
          ScheduleHandler( handler, wrt_status );
          return;
        }

        // make sure we flush the buffer before closing
        if( wrtbuff && !wrtbuff->Empty() ) WriteBlock(); //< run this in separate thread

        wrt_callback->Flash( [this,handler]( const XrdCl::XRootDStatus &status )
            {
              const size_t size = objcfg->plgr.size();
              std::vector<XrdCl::Pipeline> closes;
              closes.reserve( size );

              for( size_t i = 0; i < size; ++i )
              {
                dirs[i].CreateEOCD();
                if( !dirs[i].Empty() )
                {
                  uint32_t offset = offsets[i];
                  closes.emplace_back( XrdCl::Write( files[i], offset, dirs[i].cd_buffer.GetCursor(), dirs[i].cd_buffer.GetBuffer() ) | XrdCl::Close( files[i] ) );
                }
              }

              // now create the metadata files
              auto metactx = std::make_shared<MetaDataCtx>( objcfg.get(), dirs );
              std::vector<XrdCl::Pipeline> put_metadata;

              for( size_t i = 0; i < size; ++i )
              {
                auto file = std::make_shared<XrdCl::File>();
                std::string url = objcfg->plgr[i] + objcfg->obj + ".mt." + objcfg->mtindex + ".zip";

                XrdCl::Pipeline put = XrdCl::Open( *file, url, XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write )
                                    | XrdCl::WriteV( *file, 0, metactx->iov, metactx->iovcnt ) >> [metactx]( XrdCl::XRootDStatus& ){ }
                                    | XrdCl::Close( *file ) >> [file]( XrdCl::XRootDStatus& ){ };

                put_metadata.emplace_back( std::move( put ) );
              }

              XrdCl::Async( XrdCl::Parallel( XrdCl::Parallel( closes ), XrdCl::Parallel( put_metadata ) ) >> handler );
            } );
        wrt_callback.reset();
      }

    private:

      struct WrtCtx
      {
        WrtCtx( uint32_t checksum, const std::string &fn, const void *buffer, uint32_t bufflen ) : lfh( bufflen, checksum, fn.size() ), fn( fn), buffer( buffer ), bufflen( bufflen ), total_size( 0 )
        {
          iov[0].iov_base = lfh.get_buff();
          iov[0].iov_len  = LFH::size;
          total_size     += LFH::size;

          iov[1].iov_base = (void*)fn.c_str();
          iov[1].iov_len  = fn.size();
          total_size     += fn.size();

          iov[2].iov_base = (void*)buffer;
          iov[2].iov_len  = bufflen;
          total_size     += bufflen;
        }

        LFH               lfh;
        std::string       fn;
        const void       *buffer;
        uint32_t          bufflen;
        static const int  iovcnt = 3;
        iovec             iov[iovcnt];
        uint32_t          total_size;
      };

      struct WrtCallback
      {
          ~WrtCallback()
          {
            if( flash )
              flash( wrt_status );
          }

          void Run( const XrdCl::XRootDStatus &st = XrdCl::XRootDStatus() )
          {
            std::unique_lock<std::mutex> lck( mtx );
            if( wrt_status.IsOK() && !st.IsOK() ) wrt_status = st;
          }

          void Flash( std::function<void( const XrdCl::XRootDStatus&)> && c )
          {
            std::unique_lock<std::mutex> lck( mtx );
            flash = std::move( c );
          }

          const XrdCl::XRootDStatus& GetWrtStatus()
          {
            std::unique_lock<std::mutex> lck( mtx );
            return wrt_status;
          }


        private:

          std::mutex                                        mtx;
          XrdCl::XRootDStatus                               wrt_status;
          std::function<void( const XrdCl::XRootDStatus& )> flash;
      };

      void WriteBlock();


      std::unique_ptr<std::atomic<uint32_t>[]>     offsets;
      std::unique_ptr<WrtBuff>                     wrtbuff;
      std::vector<CentralDirectory>                dirs;
      std::shared_ptr<WrtCallback>                 wrt_callback;

      std::default_random_engine                   random_engine;

      std::mutex                                   gs_mtx;
  };

}

#endif /* SRC_XRDEC_XRDECSTRMWRITER_HH_ */
