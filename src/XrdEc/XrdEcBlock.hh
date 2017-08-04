/*
 * XrdEcBlock.hh
 *
 *  Created on: Dec 3, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECBLOCK_HH_
#define SRC_XRDEC_XRDECBLOCK_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcBlockIO.hh"

#include "XrdCl/XrdClFileSystem.hh"

#include <functional>
#include <vector>
#include <string>
#include <random>
#include <memory>

namespace XrdEc
{
  //----------------------------------------------------------------------------
  //! A block of data for writing.
  //!
  //! An instance of Block can be created using BlockPool::Create() utility.
  //! (@see BlockPool)
  //----------------------------------------------------------------------------
  class Block
  {
      //------------------------------------------------------------------------
      //! friendship with BlockPool (@see BlockPool)
      //------------------------------------------------------------------------
      friend class BlockPool;

      //------------------------------------------------------------------------
      //! friendship with BlockIO (@see BlockIO)
      //------------------------------------------------------------------------
      friend class BlockIO;

      //------------------------------------------------------------------------
      //! friendship with RedundancyProvider (@see RedundancyProvider)
      //------------------------------------------------------------------------
      friend class RedundancyProvider;

      //------------------------------------------------------------------------
      //! friendship with RepairManager (@see RepairManager)
      //------------------------------------------------------------------------
      friend class RepairManager;

    public:

      //------------------------------------------------------------------------
      //! Reset the block for new path and offset
      //!
      //! @param path      : path of the file to whom the block of data belongs to
      //! @param offset    : offset at which we are going to write
      //! @param placement : placement policy (list of host) for the whole file
      //! @param version   : respective version list
      //!
      //! @throws          : IOException if could not load existing chunk from disk
      //!
      //------------------------------------------------------------------------
      void Reset( const std::string                &path,
                        uint64_t                    offset,
                  const std::vector<placement_t>   &placement,
                  const std::vector<uint64_t>      &version );

      //------------------------------------------------------------------------
      //! Move constructor.
      //------------------------------------------------------------------------
      Block( Block && blk );

      //------------------------------------------------------------------------
      //! Destructor.
      //------------------------------------------------------------------------
      virtual ~Block();

      //------------------------------------------------------------------------
      //! Write data into block cache.
      //!
      //! @param offset : offset of the write as in the whole file (NOT block
      //!                 relative offset)
      //! @param size   : size of the buffer
      //! @param buff   : buffer with data
      //!
      //! @return       : number of bytes written into the block, once 0 is
      //!                 returned the block is full and cannot accommodate
      //!                 more data (at that point block should be synchronized
      //!                 @see Sync)
      //------------------------------------------------------------------------
      size_t Write( uint64_t offset, uint64_t size, const char *buff );

      //------------------------------------------------------------------------
      //! Truncate the block.
      //!
      //! @param size   : size after truncation
      //------------------------------------------------------------------------
      void Truncate( uint64_t size );

      //------------------------------------------------------------------------
      //! Synchronize the block with with disk.
      //!
      //! @throws : on failure IOError
      //------------------------------------------------------------------------
      void Sync();

      //------------------------------------------------------------------------
      //! Update metadata according to the status of this block
      //------------------------------------------------------------------------
      void Update( std::vector<uint64_t> &version, std::vector<placement_t> &placement )
      {
        // make sure the version is vector is of the right size
        if( blkid >= version.size() )
          version.resize( blkid + 1, 0 );
        // set the version of this block
        version[blkid] = this->version;

        // make sure the placement vector is of the right size
        if( blkid >= placement.size() )
          placement.resize( blkid + 1, placement_t() );
        // set the placement policy of this block
        placement[blkid] = this->placement;
      }

    private:

      //------------------------------------------------------------------------
      //! Constructor.
      //!
      //! @param buffer : buffer for caching writes
      //------------------------------------------------------------------------
      Block( buffer_t  &&buffer );

      //------------------------------------------------------------------------
      //! path of the file to whom the block of data belongs to
      //------------------------------------------------------------------------
      std::string  path;

      //------------------------------------------------------------------------
      //! placement policy (list of host) for this block
      //------------------------------------------------------------------------
      placement_t  placement;

      //------------------------------------------------------------------------
      //! version of this block
      //------------------------------------------------------------------------
      uint64_t  version;

      //------------------------------------------------------------------------
      //! buffer for caching writes
      //------------------------------------------------------------------------
      buffer_t  buffer;

      //------------------------------------------------------------------------
      //! Error pattern: true means chunk has been erasured,
      //! false the chunk is OK
      //------------------------------------------------------------------------
      std::vector<bool>  errpattern;

      //------------------------------------------------------------------------
      //! Index of the block (offset of the block in the whole file / data size)
      //------------------------------------------------------------------------
      uint64_t  blkid;

      //------------------------------------------------------------------------
      //! Points to the end of the data written into the buffer
      //------------------------------------------------------------------------
      uint64_t  cursor;

      //------------------------------------------------------------------------
      //! true if the data in the block have been updated after read,
      //! false otherwise
      //------------------------------------------------------------------------
      bool  updated;

      //------------------------------------------------------------------------
      //! Random number generator (for the placement policy)
      //------------------------------------------------------------------------
      std::unique_ptr<std::default_random_engine>  generator;

      //------------------------------------------------------------------------
      //! Hash function - for seeding the random number generator
      //------------------------------------------------------------------------
      static std::hash<std::string>  strhash;

      //------------------------------------------------------------------------
      //! Block I/O
      //------------------------------------------------------------------------
      BlockIO  io;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECBLOCK_HH_ */
