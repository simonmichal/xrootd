/*
 * XrdEcBlockPool.hh
 *
 *  Created on: Dec 3, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECBLOCKPOOL_HH_
#define SRC_XRDEC_XRDECBLOCKPOOL_HH_

#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcConfig.hh"

#include <mutex>
#include <condition_variable>
#include <list>
#include <algorithm>

namespace XrdEc
{
  //----------------------------------------------------------------------------
  //! Pool of memory blocks for write caching
  //----------------------------------------------------------------------------
  class BlockPool
  {
    friend class Block;

    public:

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~BlockPool()
      {

      }

      //------------------------------------------------------------------------
      //! Access to BlookPool object
      //!
      //! @return : an instance of BlockPool
      //------------------------------------------------------------------------
      inline static BlockPool& Instance()
      {
        static BlockPool blkpool;
        return blkpool;
      }

      //------------------------------------------------------------------------
      //! Factory method for creating Block objects (@see Block)
      //!
      //! @return          : a Block object
      //------------------------------------------------------------------------
      inline static Block Create()
      {
        return Instance().CreateImpl();
      }



    private:

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      BlockPool() : buffcount( 0 ),
                    maxnbbuff( 64 ) // TODO read from config
      {
      }

      //------------------------------------------------------------------------
      //! Copy constructor - deleted
      //------------------------------------------------------------------------
      BlockPool( const BlockPool& ) = delete;

      //------------------------------------------------------------------------
      //! Move constructor - deleted
      //------------------------------------------------------------------------
      BlockPool( BlockPool&& ) = delete;

      //------------------------------------------------------------------------
      //! Copy assignment operator - deleted
      //------------------------------------------------------------------------
      BlockPool& operator=( const BlockPool& ) = delete;

      //------------------------------------------------------------------------
      //! Move assignment operator - deleted
      //------------------------------------------------------------------------
      BlockPool& operator=( BlockPool&& ) = delete;

      //------------------------------------------------------------------------
      //! The actual implementation of the Create factory method (@see Create)
      //!
      //! @return          : a Block object
      //------------------------------------------------------------------------
      Block CreateImpl()
      {
        std::unique_lock<std::mutex> lck( mtx );

        // first check if we have a free buffer
        if( !buffpool.empty() )
          return FromPool();

        // if not, check if we can create a new one
        if( buffcount < maxnbbuff )
        {
          ++buffcount;
          return Block( buffer_t( Config::Instance().blksize, 0 ) );
        }

        // otherwise we have to wait until someone recycles a buffer
        while( buffpool.empty() ) cv.wait( lck );
        return FromPool();
      }

      //------------------------------------------------------------------------
      //! Recycle buffer after destruction of respective Block object (@see Block)
      //!
      //! @param buffer : the buffer that should be moved back to the pool
      //------------------------------------------------------------------------
      inline void Recycle( buffer_t &&buffer )
      {
        std::unique_lock<std::mutex> lck( mtx );
        std::fill( buffer.begin(), buffer.end(), 0 );
        buffpool.emplace_back( std::move( buffer ) );
        cv.notify_all();
      }

      //------------------------------------------------------------------------
      //! Helper function for creating Block objects from pool
      //!
      //! @return          : a Block object
      //------------------------------------------------------------------------
      inline Block FromPool()
      {
        buffer_t buffer( std::move( buffpool.back() ) );
        buffpool.pop_back();
        return Block( std::move( buffer ) );
      }

      //------------------------------------------------------------------------
      // Pool data members:
      //------------------------------------------------------------------------

      //! The pool of free buffers.
      std::list<buffer_t>     buffpool;
      //! Number of buffers in the pool (not necessary in the free pool)
      uint16_t                buffcount;
      //! maximum number of buffers
      const uint16_t          maxnbbuff;

      //------------------------------------------------------------------------
      // Synchronization primitives:
      //------------------------------------------------------------------------

      mutable std::mutex      mtx;
      std::condition_variable cv;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECBLOCKPOOL_HH_ */
