/*
 * BlockReader.cc
 *
 *  Created on: Jan 9, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcBlockReader.hh"
#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcBlockPool.hh"
#include "XrdEc/XrdEcRepairManager.hh"

namespace XrdEc
{
  //------------------------------------------------------------------------
  // Reads up to the end of given block
  //------------------------------------------------------------------------
  uint64_t BlockReader::Read( uint64_t offset, uint64_t size, char *buffer, std::future<uint64_t> &ftr )
  {
    if( size == 0 ) return 0;

    using namespace XrdCl;

    Config &cfg = Config::Instance();

    // figure out the first chunk of interest
    uint8_t  chunkid = offset / cfg.chunksize;
    // calculate how much data can we read from this block
    uint64_t rdsize = cfg.datasize - offset;
    if( rdsize > size ) rdsize = size;

    // if placement policy for this block is empty it means it
    // is a sparse file
    if( placement.empty() )
    {
      memset( buffer, 0, rdsize );
      return rdsize;
    }

    // sanity check
    if( placement.size() != cfg.nbchunks )
      throw IOError( XRootDStatus( stError, errConfig ) );

    rdctx ctx = make_rdctx( path, blkid, placement, version, offset, rdsize, ftr, repair );

    uint64_t bytesrd = 0;
    uint64_t chrdoff  = offset % cfg.chunksize;

    // lock the context so there is no race condition with repairs
    std::unique_lock<std::mutex> lck( ctx->mtx );
    while( rdsize > 0 )
    {
      if( chunkid >= placement.size() ) throw IOError( XRootDStatus( stError, errConfig ) );

      uint32_t chrdsize = chrdoff + rdsize < cfg.chunksize ? rdsize : cfg.chunksize - chrdoff;
      chbuff buff = make_chbuff( chrdoff, chrdsize, buffer );

      Async( ctx->ReadChunkPipe( chunkid, buff, ctx ) );

      buffer  += chrdsize;
      rdsize  -= chrdsize;
      bytesrd += chrdsize;

      // proceed to the next chunk
      ++chunkid;
      // reset chunk offset
      chrdoff = 0;
    }

    return bytesrd;
  }

  //----------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------
  BlockReader::BlkRdCtx::~BlkRdCtx()
  {
    // if we have failed do nothing
    if( buffers.empty() ) return; // <<<< TODO why why why ... ???

    Config &cfg = Config::Instance();

    // check if we can still repair
    if( invalid > cfg.nbparity )
    {
      // make sure we stop using user buffers before set the exception
      buffers.clear();
      std::exception_ptr exptr = std::make_exception_ptr(
          IOError( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ) ) );
      prms.set_exception( exptr );
      return;
    }

    // if there are errors use parity to repair
    if( invalid > 0 )
    {
      // make sure we have a buffer for each chunk
      if( buffers.size() < cfg.nbchunks )
        for( uint8_t chunkid = 0; chunkid < cfg.nbchunks; ++chunkid )
          if( buffers.find( chunkid ) == buffers.end() )
            buffers[chunkid] = make_chbuff();

      try
      {
        cfg.redundancy.compute( buffers );
      }
      catch( std::runtime_error &ex )
      {
        // make sure we stop using user buffers before set the exception
        buffers.clear();
        std::exception_ptr exptr = std::make_exception_ptr(
            IOError( XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errDataError ) ) );
        prms.set_exception( exptr );
        return;
      }

      // Are we suppose to repair? In principle the flag is set to true for reads
      // and to false for writes (a write might require an auxiliary read).
      if( repair )
        RepairManager::Repair( buffers, blkid, blksize, path, placement, version );
    }

    // clear the chunk buffers so the memory is being copied to user
    // buffers if necessary
    buffers.clear();

    // if the user is reading past the end of the block make sure we
    // report correct read size
    if( blksize < rdoff )
      rdsize = 0;
    else if( blksize < rdoff + rdsize )
      rdsize -= rdoff + rdsize - blksize;

    // fulfill the promise
    prms.set_value( rdsize );
  }


  //------------------------------------------------------------------------
  // Trigger additional reads so we can recover errors in the future
  //------------------------------------------------------------------------
  void BlockReader::Repair( uint8_t chunkid, chbuff &buffer, rdctx &ctx )
  {
    using namespace XrdCl;

    Config &cfg = Config::Instance();
    std::unique_lock<std::mutex> lck( ctx->mtx );

    ++ctx->invalid;
    buffer->Invalidate();

    // check if we can still repair
    if( ctx->invalid > cfg.nbparity )
      // there is nothing we can do, the number of errors exceeds the number of parity chunks
      return;

    for( uint8_t chunkid = 0; chunkid < cfg.nbchunks; ++chunkid )
    {
      // if we don't have this one read it
      if( ctx->buffers.find( chunkid ) == ctx->buffers.end() )
      {
        chbuff buff = make_chbuff();
        Async( ctx->ReadChunkPipe( chunkid, buff, ctx ) );
        // check if we have enough chunks to repair invalid ones
        if( ctx->buffers.size() - ctx->invalid >= cfg.nbdata ) break;
      }
    }
  }


  //------------------------------------------------------------------------
  // Create a pipeline that reads given chunk of the block
  //------------------------------------------------------------------------
  XrdCl::Pipeline BlockReader::BlkRdCtx::ReadChunkPipe( uint8_t chunkid, chbuff &buff, rdctx &ctx )
  {
    using namespace XrdCl;

    Config &cfg = Config::Instance();

    ctx->buffers[chunkid] = buff;
    std::string url = placement[chunkid] + '/' + path + Sufix( blkid, chunkid );

    Fwd<ChunkInfo>   rsprd;
    Fwd<std::string> rspcksum;
    Fwd<std::string> rspversion;
    Fwd<std::string> rspchid;
    Fwd<std::string> rspchsize;
    Fwd<std::string> rspblksize;

    File *file = new File();
    Pipeline pipe = Open( file, url, OpenFlags::Read )
                  | Parallel( XrdCl::Read( file, 0, cfg.chunksize, buff->Get() ) >> [rsprd]( XRootDStatus &st, ChunkInfo &chunk ){ if( st.IsOK() ) rsprd = chunk; }, // we always read the full chunk so we can compare the checksums
                              GetXAttr( file, "xrdec.checksum" ) >> [rspcksum]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) rspcksum   = value; },
                              GetXAttr( file, "xrdec.version" ) >> [rspversion]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) rspversion = value; },
                              GetXAttr( file, "xrdec.chunkid" ) >> [rspchid]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) rspchid = value; },
                              GetXAttr( file, "xrdec.chsize" ) >> [rspchsize]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) rspchsize = value; },
                              GetXAttr( file, "xrdec.blksize" ) >> [rspblksize]( XRootDStatus &st, std::string &value ){ if( st.IsOK() ) rspblksize = value; }
                    ) >> [=, &cfg]( XRootDStatus &st ) mutable
                         {
                           if( !st.IsOK() || // check if all the components of the parallel operation were successful
                               *rspchid != std::to_string( chunkid ) || // make sure it is the right chunk
                               *rspversion != std::to_string( ctx->version ) || // make sure that the version is in line with what we want
                               *rspchsize != std::to_string( rsprd->length ) || // make sure the size of the data we read is correct
                               *rspcksum != Checksum( cfg.ckstype, buff->Get(), rsprd->length ) ) // and finally make sure the checksum is OK
                           {
                             BlockReader::Repair( chunkid, buff, ctx ); // if something is wrong we need to repair the chunk
                           }
                           else
                             // Note: the actual block size might be smaller than the nominal block size
                             ctx->blksize = std::stoull( *rspblksize );
                         }
                  | XrdCl::Close( file ) >> [file]( XrdCl::XRootDStatus& ){ delete file; }; // TODO add 'Finalize( File* )' utility upstream

    return std::move( pipe );
  }


} /* namespace XrdEc */
