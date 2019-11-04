/*
 * XrdEcBlock.cc
 *
 *  Created on: Dec 3, 2018
 *      Author: simonm
 */

#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcBlockPool.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcRepairManager.hh"

#include <cstring>
#include <algorithm>

namespace XrdEc
{
  //----------------------------------------------------------------------------
  // Constructor.
  //----------------------------------------------------------------------------
  Block::Block( buffer_t && buffer ) : buffer( std::move( buffer ) ),
                                       errpattern( Config::Instance().nbchunks, false ),
                                       blkid( 0 ),
                                       cursor( 0 ),
                                       updated( false )
  {
  }

  //----------------------------------------------------------------------------
  // Move constructor.
  //----------------------------------------------------------------------------
  Block::Block( Block && blk ) : objname( std::move( blk.objname ) ),
                                 buffer( std::move( blk.buffer ) ),
                                 errpattern( std::move( blk.errpattern ) ),
                                 blkid( blk.blkid ),
                                 cursor( blk.cursor ),
                                 updated( false )
  {
  }

  //------------------------------------------------------------------------
  // Initialize block
  //------------------------------------------------------------------------
  void Block::Reset( const std::string                &objname,
                           uint64_t                    offset,
                     const placement_group            &plgr )
  {
    Config &cfg = Config::Instance();

    // initialized parameters
    this->objname   = objname;
    blkid           = offset / cfg.datasize;
    this->plgr      = plgr;
    cursor          = 0;
    updated         = false;

    // if there is an outstanding repair we have to wait
    RepairManager::Wait( blkid );

    // clear error flag for every chunk
    errpattern.clear();
    errpattern.resize( cfg.nbchunks, false );

    // clear the buffer
    memset( buffer.data(), 0, Config::Instance().blksize );

//    // if placement is not empty load from disk (data + extended attributes)
//    if( !placement.empty() ) TODO
//    {
//      // TODO first check if the data are in repair cache, if yes cancel the repair
//      io.Get( *this );
//    }
  }

  //----------------------------------------------------------------------------
  // Destructor.
  //----------------------------------------------------------------------------
  Block::~Block()
  {
    // recycle the pool in the BlockPool
    BlockPool::Instance().Recycle( std::move( buffer ) );
  }

  //------------------------------------------------------------------------
  // Write data into block cache.
  //------------------------------------------------------------------------
  size_t Block::Write( uint64_t offset, uint64_t size, const char *buff )
  {
    // make sure we are dealing with an initialized object
    if( objname.empty() )
      throw std::logic_error( "XrdEc::Block : cannot write into uninitialized block." ); // TODO

    uint64_t datasize = Config::Instance().datasize;

    // Is this the right block?
    if( blkid != offset / datasize ) return 0;

    // adjust the cursor
    uint64_t blkoff = offset % datasize;
    // adjust the size of the write if it exceeds the block size
    uint64_t left = datasize - blkoff;
    if( left < size ) size = left;

    // write into the block
    memcpy( buffer.data() + blkoff, buff, size );
    if( blkoff + size > cursor ) cursor = blkoff + size;

    return size;
  }

  //------------------------------------------------------------------------
  // Truncate the block.
  //------------------------------------------------------------------------
  void Block::Truncate( uint64_t size )
  {
    // make sure we are dealing with an initialized object
    if( objname.empty() )
      throw std::logic_error( "XrdEc::Block : cannot truncate an uninitialized block." ); // TODO

    // since we are modifying the block we have to bump the version
    if( !updated )
      updated = true;

    Config &cfg = Config::Instance();
    if( size > cfg.blksize ) size = cfg.blksize;

    if( size > cursor )
    {
      memset( buffer.data() + cursor, 0, size - cursor );
      cursor = size;
    }
    else
    {
      memset( buffer.data() + size, 0, cursor - size );
      cursor = size;
    }
  }

  //------------------------------------------------------------------------
  // Synchronize the block with with disk.
  //------------------------------------------------------------------------
  void Block::Sync()
  {
    // make sure we are dealing with an initialized object
    if( objname.empty() )
      throw IOError( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errInvalidOp ) );

    io.Put( *this );
  }

} /* namespace XrdEc */
