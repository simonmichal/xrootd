/*
 * XrdEcWrtBuff.hh
 *
 *  Created on: Oct 14, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECWRTBUFF_HH_
#define SRC_XRDEC_XRDECWRTBUFF_HH_

#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"

#include "XrdCl/XrdClBuffer.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <utility>
#include <vector>
#include <functional>

namespace XrdEc
{
  enum WrtMode { New, Overwrite };

  class WrtBuff
  {
    public:

      WrtBuff( uint64_t offset, WrtMode mode ) : mine( true ), paritybuff( Config::Instance().paritysize ), wrtmode( mode )
      {
        Config &cfg = Config::Instance();
        this->offset = offset - ( offset % cfg.datasize );
        stripes.reserve( cfg.nbchunks );
      }

      WrtBuff( WrtBuff && wrtbuff ) : offset( wrtbuff.offset ),
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

        Config &cfg = Config::Instance();
        if( wrtbuff.GetCursor() == 0 && size >= cfg.datasize )
        {
          mine = false;
          wrtbuff.Grab( const_cast<char*>( buffer ), cfg.datasize );
          wrtbuff.AdvanceCursor( cfg.datasize );
          Encode();
          if( size == cfg.datasize ) ScheduleHandler( handler );
          return cfg.datasize;
        }

        if( wrtbuff.GetSize() == 0 )
        {
          mine = true;
          wrtbuff.Allocate( cfg.datasize );
          memset( wrtbuff.GetBuffer(), 0, wrtbuff.GetSize() );
        }

        uint64_t bytesAccepted = size;
        if( wrtbuff.GetCursor() + bytesAccepted > cfg.datasize )
          bytesAccepted = cfg.datasize - wrtbuff.GetCursor();
        memcpy( wrtbuff.GetBufferAtCursor(), buffer, bytesAccepted );
        wrtbuff.AdvanceCursor( bytesAccepted );

        if( wrtbuff.GetCursor() == cfg.datasize ) Encode();

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
        Config &cfg = Config::Instance();
        wrtbuff.Allocate( cfg.datasize );
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
        Config &cfg = Config::Instance();

        // Check if it is a data chunk?
        if( strp < cfg.nbdata )
        {
          // If the cursor is at least at the expected size
          // it means we have the full chunk.
          uint64_t expsize = ( strp + 1) * cfg.chunksize;
          if( expsize <= wrtbuff.GetCursor() )
            return cfg.chunksize;

          // If the cursor is of by less than the chunk size
          // it means we have a partial chunk
          uint64_t delta = expsize - wrtbuff.GetCursor();
          if( delta < cfg.chunksize )
            return cfg.chunksize - delta;

          // otherwise we are handling an empty chunk
          return 0;
        }

        // It is a parity chunk so its size has  to be equal
        // to the size of the first chunk
        return GetStrpSize( 0 );
      }

      uint64_t GetBlkNb()
      {
        Config &cfg = Config::Instance();
        return offset / cfg.datasize;
      }

      std::string GetChecksum( uint8_t strpnb )
      {
        Config &cfg = Config::Instance();
        return CalcChecksum( stripes[strpnb].buffer, cfg.chunksize );
      }

      inline uint32_t GetBlkSize()
      {
        return wrtbuff.GetCursor();
      }

      bool Complete()
      {
        Config &cfg = Config::Instance();
        return wrtbuff.GetCursor() == cfg.datasize;
      }

      bool Empty()
      {
        return ( wrtbuff.GetSize() == 0 || wrtbuff.GetCursor() == 0 );
      }

      inline void Encode()
      {
        Config &cfg = Config::Instance();
        uint8_t i ;
        for( i = 0; i < cfg.nbdata; ++i )
          stripes.emplace_back( wrtbuff.GetBuffer( i * cfg.chunksize ), true );
        for( i = 0; i < cfg.nbparity; ++i )
          stripes.emplace_back( paritybuff.GetBuffer( i * cfg.chunksize ), false );
        cfg.redundancy.compute( stripes );
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

      uint64_t       offset;
      XrdCl::Buffer  wrtbuff;
      bool           mine;
      XrdCl::Buffer  paritybuff;
      stripes_t      stripes;
      WrtMode        wrtmode;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECWRTBUFF_HH_ */
