/*
 * XrdEcObjInterfaces.hh
 *
 *  Created on: 5 May 2020
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECOBJINTERFACES_HH_
#define SRC_XRDEC_XRDECOBJINTERFACES_HH_

#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"
#include "XrdEc/XrdEcZipUtilities.hh"

#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClFile.hh"

#include <stdint.h>
#include <string>
#include <memory>
#include <vector>

namespace XrdEc
{
  enum OpenMode
  {
    None,
    StrmRdMode,
    RandRdMode,
    StrmWrtMode,
    RandWrtMode
  };

  class ObjOperation
  {
    private:

      inline void OperationNotSupported( XrdCl::ResponseHandler *handler )
      {
        using namespace XrdCl;
        ScheduleHandler( handler, XrdCl::XRootDStatus( stError, errNotSupported ) );
      }

    public:

      ObjOperation( const ObjCfg &objcfg )
      {
        this->objcfg.reset( new ObjCfg( objcfg ) );
      }

      virtual ~ObjOperation()
      {
      }

      virtual void Open( XrdCl::ResponseHandler *handler )
      {
        OperationNotSupported( handler );
      }

      virtual void Write( uint64_t, uint32_t, const void*, XrdCl::ResponseHandler *handler )
      {
        OperationNotSupported( handler );
      }

      virtual void Read( uint64_t, uint32_t, void*, XrdCl::ResponseHandler *handler )
      {
        OperationNotSupported( handler );
      }

      virtual void Close( XrdCl::ResponseHandler *handler )
      {
        OperationNotSupported( handler );
      }

    protected:

      std::unique_ptr<ObjCfg>  objcfg;
      std::vector<std::shared_ptr<XrdCl::File>> files;
  };

  struct CentralDirectory
  {
      CentralDirectory( const ObjCfg *objcfg ) : cd_buffer( DefaultSize( objcfg ) ), cd_size( 0 ), cd_crc32c( 0 ), cd_offset( 0 ), nb_entries( 0 )
      {
      }

      CentralDirectory( CentralDirectory && cd ) : cd_buffer( std::move( cd.cd_buffer ) ), cd_size( cd.cd_size ), cd_crc32c( cd.cd_crc32c ), cd_offset( cd.cd_offset ), nb_entries( cd.nb_entries )
      {
      }

      ~CentralDirectory()
      {
      }

      inline void Add( const std::string &fn, uint32_t datasize, uint32_t checksum, uint32_t offset )
      {
        uint32_t reqsize = CDH::size + fn.size();
        // if we don't have enough space in the buffer double its size
        if( reqsize > cd_buffer.GetSize() - cd_buffer.GetCursor() )
          cd_buffer.ReAllocate( cd_buffer.GetSize() * 2 );

        // Use placement new to construct Central Directory File Header in the buffer
        new ( cd_buffer.GetBufferAtCursor() ) CDH( datasize, checksum, fn.size(), offset );
        cd_buffer.AdvanceCursor( CDH::size );

        // Copy the File Name into the buffer
        memcpy( cd_buffer.GetBufferAtCursor(), fn.c_str(), fn.size() );
        cd_buffer.AdvanceCursor( fn.size() );

        // Update the Central Directory size and offset
        cd_size += CDH::size + fn.size();
        cd_offset = offset + LFH::size + fn.size() + datasize;

        // Update the number of entries in the Central Directory
        ++nb_entries;
      }

      inline bool Empty()
      {
        return cd_buffer.GetCursor() == 0;
      }

      void CreateEOCD()
      {
        // make sure we have enough space for the End Of Central Directory record
        if( EOCD::size >  cd_buffer.GetSize() - cd_buffer.GetCursor() )
          cd_buffer.ReAllocate( cd_buffer.GetSize() + EOCD::size );

        // Now create End of Central Directory record
        new ( cd_buffer.GetBufferAtCursor() ) EOCD( nb_entries, cd_size, cd_offset );
        cd_buffer.AdvanceCursor( EOCD::size );
      }

      XrdCl::Buffer cd_buffer;
      uint32_t      cd_size;
      uint32_t      cd_crc32c;

    private:

      inline static size_t DefaultSize( const ObjCfg *objcfg )
      {
        // this should more or less give us space to accommodate 32 headers
        // witch with 32MB block corresponds to 1GB of data

        return ( CDH::size + objcfg->obj.size() + 6 /*account for the BLKID and CHID*/ ) * 32 + EOCD::size;
      }

      uint32_t               cd_offset;
      uint16_t               nb_entries;
  };


  struct MetaDataCtx
  {
      MetaDataCtx( ObjCfg *objcfg, std::vector<CentralDirectory> &dirs ) : iovcnt( 0 ), iov( nullptr ), cd( objcfg )
      {
        size_t size = objcfg->plgr.size();

        // figure out how many buffers will we need
        for( size_t i = 0; i < size; ++i )
        {
          if( dirs[i].Empty() ) continue;

          iovcnt += 3;                  // a buffer for LFH, a buffer for file name, a buffer for the binary data
        }
        ++iovcnt; // a buffer for the Central Directory

        // now once we know the size allocate the iovec
        iov = new iovec[iovcnt];
        int index = 0;
        uint32_t offset = 0;

        // add the Local File Headers and respective binary data
        for( size_t i = 0; i < size; ++i )
        {
          if( dirs[i].Empty() ) continue;

          std::string fn = objcfg->plgr[i] + objcfg->obj;
          fns.emplace_back( fn );
          lheaders.emplace_back( dirs[i].cd_size, dirs[i].cd_crc32c, fn.size() );

          // Local File Header
          iov[index].iov_base = lheaders.back().get_buff();
          iov[index].iov_len  = LFH::size;
          ++index;

          // File Name
          iov[index].iov_base = (void*)fns.back().c_str();
          iov[index].iov_len  = fns.back().size();
          ++index;

          // Binary Data
          iov[index].iov_base = dirs[i].cd_buffer.GetBuffer();
          iov[index].iov_len  = dirs[i].cd_buffer.GetCursor() - EOCD::size;
          ++index;

          // update Central Directory
          cd.Add( fn, dirs[i].cd_size, dirs[i].cd_crc32c, offset );

          // update the offset
          offset += LFH::size + fn.size() + dirs[i].cd_size;
        }

        // add the Central Directory and End Of Central Directory record
        cd.CreateEOCD();
        iov[index].iov_base = cd.cd_buffer.GetBuffer();
        iov[index].iov_len  = cd.cd_buffer.GetCursor();
        ++index;
      }

      ~MetaDataCtx()
      {
        delete[] iov;
      }

      int    iovcnt;
      iovec *iov;

    private:

      std::list<LFH>         lheaders;
      std::list<std::string> fns;
      CentralDirectory       cd;
  };
}

#endif /* SRC_XRDEC_XRDECOBJINTERFACES_HH_ */
