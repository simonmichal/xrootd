/*
 * XrdEcStrmWriter.cc
 *
 *  Created on: 5 May 2020
 *      Author: simonm
 */

#include "XrdEc/XrdEcStrmWriter.hh"
#include "XrdEc/XrdEcThreadPool.hh"

#include "XrdOuc/XrdOucCRC32C.hh"

#include <numeric>
#include <algorithm>

namespace XrdEc
{
  void StrmWriter::WriteBlock()
  {
    wrtbuff->Encode();

    std::vector<std::future<uint32_t>> checksums;
    checksums.reserve( objcfg->nbchunks );

    for( uint8_t strpnb = 0; strpnb < objcfg->nbchunks; ++strpnb )
    {
      std::future<uint32_t> ftr = ThreadPool::Instance().Execute( crc32c, 0, wrtbuff->GetChunk( strpnb ), objcfg->chunksize );
      checksums.emplace_back( std::move( ftr ) );
    }

    const size_t size = objcfg->nbchunks;
    std::vector<XrdCl::Pipeline> writes;
    writes.reserve( size );

    std::vector<size_t> fileid( size );
    std::iota( fileid.begin(), fileid.end(), 0 );
    std::shuffle( fileid.begin(), fileid.end(), random_engine );

    std::vector<size_t> spares( fileid.begin() + objcfg->nbdata, fileid.end() );

    for( size_t i = 0; i < size; ++i )
    {
      std::string fn = objcfg->obj + '.' + std::to_string( wrtbuff->GetBlkNb() ) + '.' + std::to_string( i );
      uint32_t checksum = checksums[i].get();
      std::shared_ptr<WrtCtx> wrtctx( new WrtCtx( checksum, fn, wrtbuff->GetChunk( i ), wrtbuff->GetStrpSize( i ) ) );
      uint32_t offset = offsets[ fileid[i] ].fetch_add( wrtctx->total_size );
      auto file = files[fileid[i]];
      writes.emplace_back( XrdCl::WriteV( *file, offset, wrtctx->iov, wrtctx->iovcnt ) >> [file, wrtctx, i]( XrdCl::XRootDStatus& ){ } ); // TODO fallback to spare if fails !!!
      // create respective CDH record
      dirs[fileid[i]].Add( fn, wrtbuff->GetStrpSize( i ), checksum, offset ); // TODO this needs to be updated once we know the write was successful !!!
                                                                                         // TODO once we support spares !!!
    }

    std::shared_ptr<WrtBuff> pend_buff = std::move( wrtbuff );
    std::shared_ptr<WrtCallback> _wrt_callback = wrt_callback;
    XrdCl::Async( XrdCl::Parallel( writes ) >> [pend_buff, _wrt_callback]( XrdCl::XRootDStatus &st ){ if( !st.IsOK() ) _wrt_callback->Run( st ); } );
  }
}
