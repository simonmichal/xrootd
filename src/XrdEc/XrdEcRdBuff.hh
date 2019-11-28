/*
 * XrdEcRdBuff.hh
 *
 *  Created on: Oct 18, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECRDBUFF_HH_
#define SRC_XRDEC_XRDECRDBUFF_HH_

#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"

#include "XrdCl/XrdClBuffer.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClCheckSumManager.hh"
#include "XrdCl/XrdClUtils.hh"
#include "XrdCks/XrdCksCalc.hh"

#include <utility>
#include <vector>
#include <mutex>
#include <functional>

namespace XrdEc
{
  class RdBuff
  {
      friend void ReadBlock( const ObjCfg&, const std::string&, const placement_group&, std::shared_ptr<RdBuff>&, XrdCl::ResponseHandler* );

      friend struct StrmRdCtx;

    public:

      RdBuff( const ObjCfg &objcfg, uint64_t offset, char *buffer = 0 ) : buffer( buffer ), mine( !buffer ), ready( false )
      {
        this->offset = offset - ( offset % objcfg.datasize );
        this->size   = objcfg.datasize;
        if( !buffer ) this->buffer = new char[objcfg.datasize];
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

        // check if the data are ready in the buffer
        std::unique_lock<std::mutex> lck( mtx );
        if( !ready )
        {
          // if not, register a callback for later on
          RegisterRead( offset, size, buffer, handler );
          return size;
        }
        // The data are ready - unlock the mutex!
        lck.unlock();

        memcpy( buffer, this->buffer + buffoff, size );
        ScheduleHandler( offset, size, buffer, handler );
        return size;
      }

      bool HasData( uint64_t offset )
      {
        return ( offset > this->offset ) && ( offset < this->offset + this->size );
      }

    private:

      void RegisterRead( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler )
      {
        auto callback = [offset, size, buffer, handler, this]( const XrdCl::XRootDStatus &st )
            {
              if( st.IsOK() )
              {
                uint64_t buffoff = offset - this->offset;
                memcpy( buffer, this->buffer + buffoff, size );
                ScheduleHandler( offset, size, buffer, handler );
              }
              else
                ScheduleHandler( handler, st );
            };
        pending_reads.emplace_back( std::move( callback ) );
      }

      void RunCallbacks( const XrdCl::XRootDStatus &st )
      {
        std::unique_lock<std::mutex> lck( mtx );
        ready = true;
        for( auto &cb : pending_reads ) cb( st );
      }

      uint64_t    offset;
      uint32_t    size;
      char       *buffer;
      bool        mine;

      std::mutex                        mtx;
      bool                              ready;
      std::vector<std::function<void(const XrdCl::XRootDStatus&)>> pending_reads;

  };
}


#endif /* SRC_XRDEC_XRDECRDBUFF_HH_ */
