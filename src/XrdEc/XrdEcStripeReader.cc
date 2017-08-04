/*
 * XrdEcStripeReader.cc
 *
 *  Created on: Dec 18, 2018
 *      Author: simonm
 */

#include "XrdEc/XrdEcStripeReader.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcBlockReader.hh"
#include "XrdEc/XrdEcUtilities.hh"

namespace XrdEc
{

  //------------------------------------------------------------------------
  // Read data.
  //------------------------------------------------------------------------
  size_t StripeReader::Read( uint64_t offset, uint64_t size, char *buff )
  {
    uint64_t bytesrd = 0, ret = 0;
    do
    {
      bytesrd = ReadFromBlock( offset, size, buff );
      offset += bytesrd;
      ret    += bytesrd;
      size   -= bytesrd;
      buff   += bytesrd;
    }
    while( bytesrd > 0 && size > 0 );

    IOError err{ XrdCl::XRootDStatus() };

    while( !resps.empty() )
    {
      // this is a sync read: make sure the operation is not interrupted by
      // an exception before all the asynchronous operation terminate
      try
      {
        // adjust read size according to the actual number of bytes read
        ret -= AdjustRdSize( resps.front() );
      }
      catch( IOError &ex )
      {
        if( err.Status().IsOK() )
          err = ex;
      }
      resps.pop_front();
    }

    if( !err.Status().IsOK() ) throw err;

    return ret;
  }

  //------------------------------------------------------------------------
  // Reads up to the end of current block
  //------------------------------------------------------------------------
  uint64_t StripeReader::ReadFromBlock( uint64_t offset, uint64_t size, char *buffer )
  {
    using namespace XrdCl;

    Config &cfg = Config::Instance();

    // figure out the id of the block of interest
    uint64_t blkid = offset / cfg.datasize;
    // figure out the relative offset in the block
    uint64_t blkoff  = offset % cfg.datasize;
    // calculate how much data can we read from this block
    uint64_t rdsize = cfg.datasize - blkoff;
    if( rdsize > size ) rdsize = size;

    // check if we are not reading past the end of file
    if( blkid >= placement.size() ) return 0;

    // if placement policy for this block is empty it means it
    // is a sparse file
    if( placement[blkid].empty() )
    {
      memset( buffer, 0, rdsize );
      return rdsize;
    }

    if( placement[blkid].size() != cfg.nbchunks )
      throw IOError( XRootDStatus( stError, errConfig ) );

    // if it is not the last block we are sure it will be fully filled with data
    // so we can zero initialize it, it is important because we might be repairing
    // a sparse file
    if( blkid < placement.size() - 1 )
      memset( buffer, 0, rdsize );

    std::future<uint64_t> ftr;
    BlockReader reader( path, blkid, placement[blkid], version[blkid] );
    uint64_t bytesrd = reader.Read( blkoff, rdsize, buffer, ftr );
    resps.emplace_back( blkid, bytesrd, std::move( ftr ) );

    return bytesrd;
  }

} /* namespace XrdEc */
