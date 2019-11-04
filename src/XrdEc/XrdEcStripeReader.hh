/*
 * XrdEcStripeReader.hh
 *
 *  Created on: Dec 18, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECSTRIPEREADER_HH_
#define SRC_XRDEC_XRDECSTRIPEREADER_HH_

#include "XrdEc/XrdEcUtilities.hh"

#include <list>
#include <future>

namespace XrdEc
{
  class StripeReader
  {
    public:

      //------------------------------------------------------------------------
      //! Constructor
      //!
      //! @param objname   : object name
      //! @param plgr      : placement group for the file
      //------------------------------------------------------------------------
      StripeReader( const std::string          &objname,
                    const placement_group      &plgr,
                    std::default_random_engine &generator ) :
        objname( objname ), plgr( plgr ), generator( generator )
      {
      }

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~StripeReader()
      {
      }

      //------------------------------------------------------------------------
      //! Read data.
      //!
      //! @param offset : offset of the read operation
      //! @param size   : size of the buffer
      //! @param buff   : buffer for data
      //!
      //! @return       : number of bytes read from disk
      //------------------------------------------------------------------------
      size_t Read( uint64_t offset, uint64_t size, char *buff );

    private:

      //------------------------------------------------------------------------
      //! Reads up to the end of current block
      //------------------------------------------------------------------------
      uint64_t ReadFromBlock( uint64_t offset, uint64_t size, char *buffer );

      //------------------------------------------------------------------------
      //! @see AdjustRdSize
      //!
      //! @param tpl : a tuple with arguments for AdjustRdSize
      //!
      //! @return          : the difference between rdsize and rsprdsize
      //------------------------------------------------------------------------
      inline uint64_t AdjustRdSize( std::tuple<uint64_t, uint64_t, std::future<uint64_t>>  &tpl )
      {
        uint64_t blkid = std::get<0>( tpl );
        uint64_t rdsize = std::get<1>( tpl );
        std::future<uint64_t> rsprdsize = std::move( std::get<2>( tpl ) );

        if( !rsprdsize.valid() ) return 0;
        uint64_t adjustment = rdsize - rsprdsize.get();

        if( blkid != plgr.size() - 1 ) return 0;

        return adjustment;
      }

      //------------------------------------------------------------------------
      //! path of the file to whom the block of data belongs to
      //------------------------------------------------------------------------
      std::string  objname;

      //------------------------------------------------------------------------
      //! placement policy for each block
      //------------------------------------------------------------------------
      const placement_group  &plgr;

      std::default_random_engine &generator;

      //------------------------------------------------------------------------
      //! List of read responses from particular blocks. A future contains the
      //! actual number of data read from the block. Note: the future might not
      //! be valid in case it is a non-existent block in  a sparse file.
      //------------------------------------------------------------------------
      std::list<std::tuple<uint64_t, uint64_t, std::future<uint64_t>>> resps;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECSTRIPEREADER_HH_ */
