/*
 * XrdEcBlockIO.cc
 *
 *  Created on: Dec 4, 2018
 *      Author: simonm
 */

#include "XrdEc/XrdEcBlockIO.hh"
#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcBlockReader.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"

#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"

namespace XrdEc
{
  //----------------------------------------------------------------------------
  // Populates given block with data from disk
  //----------------------------------------------------------------------------
  void BlockIO::Get( Block &block )
  {
//    // we will need configuration details
//    Config  &cfg = Config::Instance();
//
//    BlockReader reader( block.objname, block.blkid, block.placement, block.version, false );
//    std::future<uint64_t> ftr;
//    reader.Read( 0, cfg.datasize, block.buffer.data(), ftr );
//    if( ftr.valid() ) block.cursor = ftr.get();
  }

  //----------------------------------------------------------------------------
  // Preserves given block on disk
  //----------------------------------------------------------------------------
  void BlockIO::Put( Block &block )
  {
    using namespace XrdCl;

    // we will need configuration details
    Config &cfg = Config::Instance();

    // compute parity
    auto parity_begin = block.errpattern.begin() + cfg.nbdata;
    std::fill( parity_begin, block.errpattern.end(), true );
    auto chunks = ToChunks( block );
    cfg.redundancy.compute( chunks );
    chunks.clear();

    std::list<resp_t> rspput;
    for( uint8_t i = 0; i < cfg.nbchunks; ++i )
    {
      std::future<XRootDStatus> rsp = PlaceChunk( block, i, false );
      rspput.emplace_back( std::move( rsp ), i );
    }

    uint8_t relocate = 0;
    do
    {
      resp_t rsp( std::move( rspput.front() ) );
      rspput.pop_front();
      XRootDStatus status = std::get<0>( rsp ).get();
      if( !status.IsOK() )
      {
        // TODO log the error

        if( relocate > cfg.maxrelocate )
          throw IOError( XRootDStatus( stError, errNoMoreReplicas ) ); // TODO probably not the perfect error code

        uint8_t chunkid = std::get<1>( rsp );
        rspput.emplace_back( PlaceChunk( block, chunkid, true ), chunkid );
        ++relocate;
      }
    }
    while( !rspput.empty() );
  }

  //----------------------------------------------------------------------------
  // Converts the data buffer from Block object into chunk buffers
  //----------------------------------------------------------------------------
  std::future<XrdCl::XRootDStatus> BlockIO::PlaceChunk( Block &block, uint8_t chunkid, bool relocate )
  {
    return std::future<XrdCl::XRootDStatus>();

//    using namespace XrdCl;
//
//    OpenFlags::Flags flags = Place( chunkid, block.placement, *block.generator, block.plgr, relocate );
//
//    Config &cfg = Config::Instance();
//
//    uint64_t  blkoff = chunkid * cfg.chunksize;
//    char     *chunk  = block.buffer.data() + blkoff;
//    // Note: if we are dealing with data chunk calculate chunk size based on the
//    // relative offset in the block, if we are dealing with parity chunks use the
//    // size of the first chunk (the biggest one).
//    uint64_t  off    = chunkid >= cfg.nbdata ? 0 : blkoff;
//    uint64_t  chsize = ( block.cursor >= off + cfg.chunksize )
//                     ? cfg.chunksize
//                     : ( block.cursor > off ) ? block.cursor - off : 0;
//
//    std::string url = block.placement[chunkid] + '/' + block.path + Sufix( block.blkid, chunkid );
//    std::string checksum = Checksum( cfg.ckstype, chunk, chsize );
//
//    File *file = new File();
//    Pipeline putchunk = Open( file, url, flags )
//                      | Parallel( Write( file, 0 , chsize, chunk ),
//                                  SetXAttr( file, "xrdec.checksum", checksum ),
//                                  SetXAttr( file, "xrdec.version", std::to_string( block.version ) ),
//                                  SetXAttr( file, "xrdec.chunkid", std::to_string( chunkid ) ),
//                                  SetXAttr( file, "xrdec.chsize", std::to_string( chsize ) ),
//                                  SetXAttr( file, "xrdec.blksize", std::to_string( block.cursor ) ) )
//                      | Close( file ) >> [file]( XRootDStatus& ){ delete file; };
//
//    return Async( std::move( putchunk ) );
  }

  //----------------------------------------------------------------------------
  // Places the chunk with given ID from the given block on disk
  //----------------------------------------------------------------------------
  std::unordered_map<uint8_t, chbuff> BlockIO::ToChunks( Block &block )
  {
    Config &cfg = Config::Instance();
    char *buffer = block.buffer.data();
    std::unordered_map<uint8_t, chbuff> ret;

    for( uint8_t chunkid = 0; chunkid < cfg.nbchunks; ++chunkid, buffer += cfg.chunksize )
    {
      ret[chunkid] = make_chbuff( 0, cfg.chunksize, buffer );
      if( block.errpattern[chunkid] ) ret[chunkid]->Invalidate();
    }

    return std::move( ret );
  }

} /* namespace XrdEc */
