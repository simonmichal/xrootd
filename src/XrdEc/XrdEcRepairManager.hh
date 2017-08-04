/*
 * XrdEcRepairManager.hh
 *
 *  Created on: Feb 8, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECREPAIRMANAGER_HH_
#define SRC_XRDEC_XRDECREPAIRMANAGER_HH_

#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcBlock.hh"

#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

#include "XrdSys/XrdSysPthread.hh"

#include <stdint.h>

#include <memory>
#include <unordered_map>
#include <mutex>

namespace XrdEc
{
  class RepairManager
  {
      class FinalizeRepair
      {
        public:

          FinalizeRepair( uint64_t blkid ) : blkid( blkid )
          {

          }

          ~FinalizeRepair()
          {
            RepairManager &mgr = Instance();
            // lock the repair manager
            std::unique_lock<std::mutex> lck( mgr.mtx );
            // if the block is not longer pending there's nothing to be done
            auto itr = mgr.pending.find( blkid );
            if( itr == mgr.pending.end() ) return;
            itr->second->Post();
            mgr.pending.erase( itr );
          }

        private:

          uint64_t blkid;
      };

    public:

      static void Repair( std::unordered_map<uint8_t, chbuff> &buffers, uint64_t blkid, uint64_t blksize, const std::string &path, const placement_t &placement, uint64_t version )
      {
        RepairManager &mgr = Instance();
        // lock the repair manager
        std::unique_lock<std::mutex> lck( mgr.mtx );
        // check if we are already trying to repair this block
        if( mgr.pending.count( blkid ) || mgr.finalizing ) return;

        // create synchronization utility
        std::shared_ptr<FinalizeRepair> finalize = std::make_shared<FinalizeRepair>( blkid );
        mgr.pending.emplace( blkid, std::make_shared<XrdSysSemaphore>( 0 ) );

        using namespace XrdCl;

        Config &cfg = Config::Instance();

        for( uint8_t chunkid = 0; chunkid < cfg.nbchunks; ++chunkid )
        {
          if( buffers[chunkid]->IsValid() ) continue;

          std::shared_ptr<File> file = std::make_shared<File>();
          std::string url = placement[chunkid] + '/' + path + Sufix( blkid, chunkid );
          uint64_t  blkoff = chunkid * cfg.chunksize;
          chbuff    chunk   = make_chbuff();
          memcpy( chunk->Get(), buffers[chunkid]->Get(), cfg.chunksize );
          // Note: if we are dealing with data chunk calculate chunk size based on the
          // relative offset in the block, if we are dealing with parity chunks use the
          // size of the first chunk (the biggest one).
          uint64_t  off    = chunkid >= cfg.nbdata ? 0 : blkoff;
          uint64_t  chsize = ( blksize >= off + cfg.chunksize )
                           ? cfg.chunksize
                           : ( blksize > off ) ? blksize - off : 0;
          std::string checksum = Checksum( cfg.ckstype, chunk->Get(), chsize );

          Pipeline repair = Open( *file, url, OpenFlags::Delete | OpenFlags::Write )
                          | Parallel( Write( *file, 0, chsize, chunk->Get() ) >> [chunk]( XRootDStatus& ){ },
                                      SetXAttr( *file, "xrdec.checksum", checksum ),
                                      SetXAttr( *file, "xrdec.version", std::to_string( version ) ),
                                      SetXAttr( *file, "xrdec.chunkid", std::to_string( chunkid ) ),
                                      SetXAttr( *file, "xrdec.chsize", std::to_string( chsize ) ),
                                      SetXAttr( *file, "xrdec.blksize", std::to_string( blksize ) ) )
                          | Close( *file ) >> [file, finalize]( XRootDStatus &st ){ };

          Async( std::move( repair ) );
        }
      }

      static void Wait( uint64_t blkid )
      {
        std::shared_ptr<XrdSysSemaphore> sem;
        { // critical section
          RepairManager &mgr = Instance();
          // lock the repair manager
          std::unique_lock<std::mutex> lck( mgr.mtx );
          // if the block is not under repair there is nothing to be done
          auto itr = mgr.pending.find( blkid );
          if( itr == mgr.pending.end() ) return;
          // get the semaphore
          sem = mgr.pending[blkid];
        }
        // wait for the repair to finish
        sem->Wait();
      }

      static void Finalize()
      {
        RepairManager &mgr = Instance();

        { // critical section
          std::unique_lock<std::mutex> lck( mgr.mtx );
          // lets make sure no new repairs will be submitted
          mgr.finalizing = true;
        }

        // lets loop until there are no pending repairs,
        // the termination condition is inside of the loop
        while( true )
        {
          std::shared_ptr<XrdSysSemaphore> sem;
          { // critical section
            std::unique_lock<std::mutex> lck( mgr.mtx );
            // Are we done yet?
            if( mgr.pending.empty() ) return;
            sem = mgr.pending.begin()->second;
          }
          sem->Wait();
        }
      }

    private:

      static RepairManager& Instance()
      {
        static RepairManager mgr;
        return mgr;
      }

      RepairManager() : finalizing( false )
      {
      }

      ~RepairManager()
      {
      }

      RepairManager( const RepairManager& ) = delete;
      RepairManager( RepairManager&& ) = delete;

      RepairManager& operator=( const RepairManager& ) = delete;
      RepairManager* operator=( RepairManager&& ) = delete;

      // there is at most one writer so we can use a semaphore
      std::unordered_map<uint64_t, std::shared_ptr<XrdSysSemaphore>> pending;

      bool finalizing;

      mutable std::mutex mtx;
  };

}


#endif /* SRC_XRDEC_XRDECREPAIRMANAGER_HH_ */
