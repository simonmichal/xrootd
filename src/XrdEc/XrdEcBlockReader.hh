/*
 * BlockReader.hh
 *
 *  Created on: Jan 9, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECBLOCKREADER_HH_
#define SRC_XRDEC_XRDECBLOCKREADER_HH_

#include "XrdEc/XrdEcUtilities.hh"

#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

#include <exception>
#include <memory>
#include <future>
#include <atomic>
#include <unordered_map>
#include <mutex>

namespace XrdEc
{
  //----------------------------------------------------------------------------
  //! Forward declaration of BlkRdCtx
  //----------------------------------------------------------------------------
  class BlkRdCtx;

  //----------------------------------------------------------------------------
  //! Utility class for reading data from a block
  //----------------------------------------------------------------------------
  class BlockReader
  {
    public:

      //------------------------------------------------------------------------
      //! Constructor
      //!
      //! @param path      : path of the file
      //! @param placement : placement policy for given block of data
      //! @param version   : version of the given block
      //------------------------------------------------------------------------
      BlockReader( const std::string  &path,
                         uint64_t      blkid,
                   const placement_t  &placement,
                         uint64_t      version,
                         bool          repair = true ) :
        path( path ), blkid( blkid ), placement( placement ), version( version ),
        repair( repair )
      {

      }

      //------------------------------------------------------------------------
      //! Reads up to the end of given block
      //!
      //! @param offset : block relative offset of the read
      //! @param size   : block relative size of the read
      //! @param ftr    : the actual number of bytes read from the block (Note:
      //!                 might be smaller than expected as the actual block size
      //!                 might be smaller than the nominal block size)
      //!
      //! @return       : expected read size
      //------------------------------------------------------------------------
      uint64_t Read( uint64_t offset, uint64_t size, char *buffer, std::future<uint64_t> &ftr );

    private:

      //------------------------------------------------------------------------
      //! Helper class for managing common context of concurrent reads from
      //! data chunks.
      //!
      //! In case of a failure triggers repairs from parity chunks.
      //------------------------------------------------------------------------
      struct BlkRdCtx
      {
        //----------------------------------------------------------------------
        //! Constructor
        //!
        //! @param path      : path of the file
        //! @param placement : placement policy for given block of data
        //! @param version   : version of the given block
        //! @param rdoff  : block relative offset of the read
        //! @param rdsize : block relative size of the read
        //! @param ftr    : the actual amount of bytes read
        //----------------------------------------------------------------------
        BlkRdCtx( const std::string            &path,
                        uint64_t                blkid,
                  const placement_t            &placement,
                        uint64_t                version,
                        uint64_t                rdoff,
                        uint64_t                rdsize,
                        std::future<uint64_t>  &ftr,
                        bool                    repair) :
          path( path ), blkid( blkid ), placement( placement ), version( version ),
          rdoff( rdoff ), rdsize( rdsize ), blksize( 0 ), invalid( 0 ), repair( repair )
        {
          ftr = prms.get_future();
        }

        //----------------------------------------------------------------------
        //! Destructor
        //!
        //! Repairs broken chunks if possible, copies data to user buffers if
        //! necessary and fulfills the promise.
        //----------------------------------------------------------------------
        ~BlkRdCtx();

        //------------------------------------------------------------------------
        //! Create a pipeline that reads given chunk of the block
        //!
        //! @param chunkid : ID of the chunk
        //! @param buff    : chunk buffer for the data
        //! @param ctx     : context of the read operation
        //------------------------------------------------------------------------
        XrdCl::Pipeline ReadChunkPipe( uint8_t chunkid, chbuff &buff, std::shared_ptr<BlkRdCtx> &ctx );

        // path of the file to whom the block of data belongs to
        std::string  path;
        // block ID
        uint64_t blkid;
        // placement policy for our block
        const placement_t  &placement;
        // version our block block
        const uint64_t  version;
        // offset of the read within given block
        uint64_t rdoff;
        // size of the read within given block
        uint64_t rdsize;
        // actual size of the block
        std::atomic<uint64_t> blksize;
        // the promised result of read from block operation
        std::promise<uint64_t> prms;

        std::unordered_map<uint8_t, chbuff> buffers;
        // keep track of the number of invalid chunks
        uint8_t invalid;
        // if true blocks will be automatically repaired
        bool repair;

        std::mutex mtx;
      };

      //------------------------------------------------------------------------
      //! An alias for a shared pointer to BlkRdCtx
      //------------------------------------------------------------------------
      typedef std::shared_ptr<BlkRdCtx> rdctx;

      //------------------------------------------------------------------------
      //! Utility function for creating the BlkRdCtx
      //------------------------------------------------------------------------
      inline rdctx make_rdctx( const std::string &path, uint64_t blkid,
                               const placement_t &placement, uint64_t version,
                               uint64_t rdoff, uint64_t rdsize,
                               std::future<uint64_t> &ftr, bool repair )
      {
        return std::make_shared<BlkRdCtx>( path, blkid, placement, version, rdoff, rdsize, ftr, repair );
      }

      //------------------------------------------------------------------------
      //! Trigger additional reads so we can recover errors in the future
      //!
      //! @param chunkid : chunk ID
      //! @param ctx     : context of the read operation
      //------------------------------------------------------------------------
      static void Repair( uint8_t chunkid, chbuff &buff, rdctx &ctx );

      //------------------------------------------------------------------------
      //! path of the file to whom the block of data belongs to
      //------------------------------------------------------------------------
      std::string  path;

      //------------------------------------------------------------------------
      //! block ID
      //------------------------------------------------------------------------
      uint64_t blkid;

      //------------------------------------------------------------------------
      //! placement policy for our block
      //------------------------------------------------------------------------
      const placement_t  &placement;

      //------------------------------------------------------------------------
      //! version our block block
      //------------------------------------------------------------------------
      const uint64_t  version;

      //------------------------------------------------------------------------
      //! if true blocks will be automatically repaired during read operation
      //------------------------------------------------------------------------
      bool repair;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECBLOCKREADER_HH_ */
