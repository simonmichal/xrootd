/*
 * XrdEcStore.cc
 *
 *  Created on: Dec 3, 2018
 *      Author: simonm
 */

#include "XrdEc/XrdEcDataStore.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcBlockPool.hh"
#include "XrdEc/XrdEcRepairManager.hh"

#include "XrdCl/XrdClOperations.hh"
#include "XrdCl/XrdClFileSystemOperations.hh"
#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"
#include "XrdCl/XrdClFileSystem.hh"

#include <memory>


namespace XrdEc
{
  //----------------------------------------------------------------------------
  // Initialize the hash function
  //----------------------------------------------------------------------------
  std::hash<std::string> DataStore::strhash;

  void DataStore::RemoveBlocks( uint64_t nbblk )
  {
    // TODO

//    using namespace XrdCl;
//
//    Config &cfg = Config::Instance();
//
//    std::vector<std::future<XRootDStatus>> ftrs; ftrs.reserve( ( placement.size() - nbblk ) * cfg.nbchunks );
//
//    while( placement.size() > nbblk )
//    {
//      uint64_t blkid = placement.size() - 1;
//
//      // if it is a non-existent block simply pop the placement
//      if( placement[blkid].empty() )
//      {
//        placement.pop_back();
//        version.pop_back();
//        continue;
//      }
//
//      for( uint8_t chunkid = 0; chunkid < cfg.nbchunks; ++chunkid )
//      {
//        URL url( placement[blkid][chunkid] + '/' + path + Sufix( blkid, chunkid ) );
//        std::shared_ptr<FileSystem> fs = std::make_shared<FileSystem>( url.GetURL() );
//        ftrs.emplace_back( Async( Rm( *fs, url.GetPath() ) >> [fs]( XRootDStatus& ){ } ) );
//      }
//      placement.pop_back();
//      version.pop_back();
//    }
//
//    XRootDStatus status;
//    while( !ftrs.empty() )
//    {
//      std::future<XRootDStatus> ftr = std::move( ftrs.back() );
//      ftrs.pop_back();
//      XRootDStatus st = ftr.get();
//      if( st.IsOK() || ( st.code == errErrorResponse && st.errNo == kXR_NotFound ) ) continue;
//      // TODO in the future we should probably just log this and ignore all
//      // errors
//      // important thing is metadata gets updated and this chunk is no longer
//      // available to the user
//      if( status.IsOK() )
//        status = st;
//    }
//
//    if( !status.IsOK() )
//      throw IOError( status );
  }

  void DataStore::TruncateBlock( uint64_t blkid, uint64_t datasize )
  {
    // TODO

//    Config &cfg = Config::Instance();
//    uint64_t offset = blkid * cfg.datasize;
//
//    Block blk( BlockPool::Create() );
//    blk.Reset( path, offset, plgr, placement, version );
//    blk.Truncate( datasize );
//    blk.Sync();
//    blk.Update( version, placement );
  }

  void DataStore::AddEmptyBlocks( uint64_t fullblk, uint64_t lstblk )
  {
    // TODO

//    using namespace XrdCl;
//
//    Config &cfg = Config::Instance();
//
//    // we wont actually create those blocks, we will rather use
//    // the sparse file functionality
//    placement.resize( fullblk );
//    version.resize( fullblk, 0 );
//
//    if( lstblk )
//    {
//      placement.emplace_back();
//      version.emplace_back( 0 );
//    }
//    else
//      lstblk = cfg.blksize;
//
//    // adjust the size of last block
//    TruncateBlock( placement.size() - 1, lstblk );
  }

  void DataStore::Finalize()
  {
    RepairManager::Finalize();
  }

} /* namespace XrdEc */
