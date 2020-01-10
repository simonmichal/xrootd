/*
 * XrdEcWrtBuff.hh
 *
 *  Created on: Oct 14, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECWRTBUFF_HH_
#define SRC_XRDEC_XRDECWRTBUFF_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"
#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdEc/XrdEcConfig.hh"

#include "XrdCl/XrdClBuffer.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <utility>
#include <vector>
#include <functional>

#include <iostream>

namespace XrdEc
{
  enum WrtMode { New, Overwrite };

  class WrtBuff
  {
    public:

      WrtBuff( const ObjCfg &objcfg, uint64_t offset, WrtMode mode ) : objcfg( objcfg ), mine( true ), paritybuff( objcfg.paritysize ), wrtmode( mode )
      {
        this->offset = offset - ( offset % objcfg.datasize );
        stripes.reserve( objcfg.nbchunks );
      }

      WrtBuff( WrtBuff && wrtbuff ) : objcfg( wrtbuff.objcfg ),
                                      offset( wrtbuff.offset ),
                                      wrtbuff( std::move( wrtbuff.wrtbuff ) ),
                                      mine( wrtbuff.mine ),
                                      paritybuff( std::move( wrtbuff.paritybuff ) ),
                                      stripes( std::move( wrtbuff.stripes ) ),
                                      wrtmode( wrtbuff.wrtmode )
      {
      }

      ~WrtBuff()
      {
        if( !mine ) wrtbuff.Release();
      }

      uint32_t Write( uint64_t offset, uint32_t size, const char *buffer, XrdCl::ResponseHandler *handler )
      {
        if( this->offset + wrtbuff.GetCursor() != offset ) throw std::exception();

        // TODO : for now we comment this out as we cannot rely on the user
        //        buffer after calling user handler !!!
//        if( wrtbuff.GetCursor() == 0 && size >= objcfg.datasize )
//        {
//          mine = false;
//          wrtbuff.Grab( const_cast<char*>( buffer ), objcfg.datasize );
//          wrtbuff.AdvanceCursor( objcfg.datasize );
//          Encode();
//          if( size == objcfg.datasize ) ScheduleHandler( handler );
//          return objcfg.datasize;
//        }

        if( wrtbuff.GetSize() == 0 )
        {
          mine = true;
          wrtbuff.Allocate( objcfg.datasize );
          memset( wrtbuff.GetBuffer(), 0, wrtbuff.GetSize() );
        }

        uint64_t bytesAccepted = size;
        if( wrtbuff.GetCursor() + bytesAccepted > objcfg.datasize )
          bytesAccepted = objcfg.datasize - wrtbuff.GetCursor();
        memcpy( wrtbuff.GetBufferAtCursor(), buffer, bytesAccepted );
        wrtbuff.AdvanceCursor( bytesAccepted );

        if( bytesAccepted == size ) ScheduleHandler( handler );

        return bytesAccepted;
      }

      void Pad( uint32_t size )
      {
        // if the buffer exist we only need to move the cursor
        if( wrtbuff.GetSize() != 0 )
        {
          wrtbuff.AdvanceCursor( size );
          return;
        }

        // otherwise we allocate the buffer and set the cursor
        mine = true;
        wrtbuff.Allocate( objcfg.datasize );
        memset( wrtbuff.GetBuffer(), 0, wrtbuff.GetSize() );
        wrtbuff.SetCursor( size );
        return;
      }

      void* GetChunk( uint8_t strpnb )
      {
        return stripes[strpnb].buffer;
      }

      uint32_t GetStrpSize( uint8_t strp )
      {
        // Check if it is a data chunk?
        if( strp < objcfg.nbdata )
        {
          // If the cursor is at least at the expected size
          // it means we have the full chunk.
          uint64_t expsize = ( strp + 1) * objcfg.chunksize;
          if( expsize <= wrtbuff.GetCursor() )
            return objcfg.chunksize;

          // If the cursor is of by less than the chunk size
          // it means we have a partial chunk
          uint64_t delta = expsize - wrtbuff.GetCursor();
          if( delta < objcfg.chunksize )
            return objcfg.chunksize - delta;

          // otherwise we are handling an empty chunk
          return 0;
        }

        // It is a parity chunk so its size has  to be equal
        // to the size of the first chunk
        return GetStrpSize( 0 );
      }

      uint64_t GetBlkNb()
      {
        return offset / objcfg.datasize;
      }

      std::string GetChecksum( uint8_t strpnb )
      {
        return CalcChecksum( stripes[strpnb].buffer, objcfg.chunksize );
      }

      inline uint32_t GetBlkSize()
      {
        return wrtbuff.GetCursor();
      }

      bool Complete()
      {
        return wrtbuff.GetCursor() == objcfg.datasize;
      }

      bool Empty()
      {
        return ( wrtbuff.GetSize() == 0 || wrtbuff.GetCursor() == 0 );
      }

      inline void Encode()
      {

        uint8_t i ;
        for( i = 0; i < objcfg.nbdata; ++i )
          stripes.emplace_back( wrtbuff.GetBuffer( i * objcfg.chunksize ), true );
        for( i = 0; i < objcfg.nbparity; ++i )
          stripes.emplace_back( paritybuff.GetBuffer( i * objcfg.chunksize ), false );
        Config &cfg = Config::Instance();
        cfg.GetRedundancy( objcfg ).compute( stripes );
      }

      WrtMode GetWrtMode()
      {
        return wrtmode;
      }

      uint64_t GetOffset()
      {
        return offset;
      }

    private:

      ObjCfg         objcfg;
      uint64_t       offset;
      XrdCl::Buffer  wrtbuff;
      bool           mine;
      XrdCl::Buffer  paritybuff;
      stripes_t      stripes;
      WrtMode        wrtmode;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECWRTBUFF_HH_ */
