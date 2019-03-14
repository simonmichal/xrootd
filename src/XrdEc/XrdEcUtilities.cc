/*
 * XrdEcUtilities.cc
 *
 *  Created on: Jan 10, 2019
 *      Author: simonm
 */



#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcConfig.hh"

#include "XrdCl/XrdClCheckSumManager.hh"
#include "XrdCl/XrdClUtils.hh"

#include "XrdCks/XrdCksCalc.hh"

namespace XrdEc
{
  //------------------------------------------------------------------------
  // Constructor.
  //------------------------------------------------------------------------
  ChunkBuffer::ChunkBuffer( uint64_t offset, uint64_t size, char* dst ):
    valid( true ), offset( offset ), size( size ), userbuff( dst )
  {
    Config &cfg = Config::Instance();

    // check if the user buffer is big enogh to acomodate a whole chunk
    if( offset != 0 || size != cfg.chunksize || !dst )
    {
      // if not, allocate memory
      buffer.reset( new char[cfg.chunksize] );
      memset( buffer.get(), 0, cfg.chunksize );
    }
    else
    // otherwise we don't need extra memory
      buffer = nullptr;
  }


  //----------------------------------------------------------------------------
  // Calculates checksum of given type from given buffer
  //----------------------------------------------------------------------------
  std::string Checksum( const std::string &type, void *buffer, uint32_t size )
  {
    using namespace XrdCl;

    CheckSumManager *cksMan = DefaultEnv::GetCheckSumManager();
    XrdCksCalc *cksCalcObj  = cksMan->GetCalculator( type );
    cksCalcObj->Update( reinterpret_cast<const char*>( buffer ), size );

    int          calcSize = 0;
    std::string  calcType = cksCalcObj->Type( calcSize );

    XrdCksData ckSum;
    ckSum.Set( calcType.c_str() );
    ckSum.Set( (void*)cksCalcObj->Final(), calcSize );
    char *cksBuffer = new char[265];
    ckSum.Get( cksBuffer, 256 );
    std::string checkSum  = calcType + ":";
    checkSum += Utils::NormalizeChecksum( calcType, cksBuffer );
    delete [] cksBuffer;
    delete cksCalcObj;

    return checkSum;
  }

  //------------------------------------------------------------------------
  // Find a new location (host) for given chunk.
  //------------------------------------------------------------------------
  XrdCl::OpenFlags::Flags Place( uint8_t                      chunkid,
                                 placement_t                 &placement,
                                 std::default_random_engine  &generator,
                                 const placement_group       &plgr,
                                 bool                         relocate )
  {
    Config &cfg = Config::Instance();

    static std::uniform_int_distribution<uint32_t>  distribution( 0, plgr.size() - 1 );

    bool exists = !placement.empty() && !placement[chunkid].empty();

    XrdCl::OpenFlags::Flags flags = XrdCl::OpenFlags::Write |
        ( exists ? XrdCl::OpenFlags::Delete : XrdCl::OpenFlags::New );

    if( !relocate && exists ) return flags;

    if( placement.empty() ) placement.resize( cfg.nbchunks, "" );

    std::string host;
    do
    {
      host = plgr[distribution( generator )];
    }
    while( std::count( placement.begin(), placement.end(), host ) );

    placement[chunkid] = host;

    return flags;
  }
}
