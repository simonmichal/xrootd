/*
 * XrdEcBlockIO.hh
 *
 *  Created on: Dec 4, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECBLOCKIO_HH_
#define SRC_XRDEC_XRDECBLOCKIO_HH_

#include "XrdEc/XrdEcUtilities.hh"

#include <tuple>
#include <unordered_map>
#include <future>

namespace XrdEc
{
  //----------------------------------------------------------------------------
  // Forward declaration of Block class
  //----------------------------------------------------------------------------
  class Block;

  //----------------------------------------------------------------------------
  //! BlockIO utility provides Get/Put semantics for Block objects
  //----------------------------------------------------------------------------
  class BlockIO
  {
      //------------------------------------------------------------------------
      //! friendship with RepairManager (@see RepairManager)
      //------------------------------------------------------------------------
      friend class RepairManager;

      //------------------------------------------------------------------------
      //! alias for a tuple consisting of future status of chunk operation and
      //! a chunk ID
      //------------------------------------------------------------------------
      typedef std::tuple<std::future<XrdCl::XRootDStatus>, uint8_t> resp_t;

    public:

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      BlockIO()
      {
      }

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~BlockIO()
      {
      }

      //------------------------------------------------------------------------
      //! Populates given block with data from disk
      //!
      //! @param block : the block in question
      //------------------------------------------------------------------------
      void Get( Block &block );

      //------------------------------------------------------------------------
      //! Calculates parity and persists given block on disk
      //!
      //! @param block : the block in question
      //------------------------------------------------------------------------
      void Put( Block &block );

    private:

      //------------------------------------------------------------------------
      //! Converts the data buffer from Block object into chunk buffers
      //!
      //! @param block : the block in question
      //! @return      : a mapping chunk ID to chunk buffer
      //------------------------------------------------------------------------
      inline static std::unordered_map<uint8_t, chbuff> ToChunks( Block &block );

      //------------------------------------------------------------------------
      //! Places the chunk with given ID from the given block on disk
      //!
      //! @param block   : the block in question
      //! @param chunkid : the chunk to be placed on disk
      //! TODO
      //! @return        : future status of the operation
      //------------------------------------------------------------------------
      static std::future<XrdCl::XRootDStatus> PlaceChunk( Block   &block,
                                                          uint8_t  chunkid,
                                                          bool     relocate);
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECBLOCKIO_HH_ */
