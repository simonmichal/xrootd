/*
 * XrdEcRdBuff.hh
 *
 *  Created on: Oct 18, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECRDBUFF_HH_
#define SRC_XRDEC_XRDECRDBUFF_HH_

#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"

#include "XrdCl/XrdClBuffer.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <utility>
#include <vector>

namespace XrdEc
{
  class RdBuff
  {
      friend void ReadBlock( const std::string&, const std::string&, const placement_group&, std::shared_ptr<RdBuff>&, XrdCl::ResponseHandler* );

      friend struct StrmRdCtx;

    public:

      RdBuff( uint64_t offset, char *buffer = 0 ) : buffer( buffer ), mine( !buffer ), empty( true )
      {
        Config &cfg  = Config::Instance();
        this->offset = offset - ( offset % cfg.datasize );
        this->size   = cfg.datasize;
        if( !buffer ) buffer = new char[cfg.datasize];
      }

      ~RdBuff()
      {
        if( mine ) delete[] buffer;
      }

      uint32_t Read( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler )
      {
        // check if we can serve some of the data from our read buffer
        if( this->offset > offset ||
            offset > this->offset + this->size ) return 0;


        uint64_t buffoff = offset - this->offset;
        if( size > this->size - buffoff )
          size = this->size - buffoff;

        memcpy( buffer, this->buffer, size );
        ScheduleHandler( offset, size, buffer, handler );
        return size;
      }

      bool HasData( uint64_t offset )
      {
        return ( offset > this->offset ) && ( offset < this->offset + this->size );
      }

      bool Empty()
      {
        return empty;
      }

    private:

      uint64_t  offset;
      uint32_t  size;
      char     *buffer;
      bool      mine;
      bool      empty;
  };
}


#endif /* SRC_XRDEC_XRDECRDBUFF_HH_ */
