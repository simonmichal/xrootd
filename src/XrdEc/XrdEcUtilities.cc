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
  LocationStatus ToLocationStatus( const std::string &str )
  {
    if( str == "rw" ) return rw;
    if( str == "ro" ) return ro;
    if( str == "drain" ) return drain;
    if( str == "off" ) return off;
    throw std::exception(); // TODO
  }

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
    LocationStatus lst;
    do
    {
      auto &tpl = plgr[distribution( generator )];
      host = std::get<0>( tpl );
      lst  = std::get<1>( tpl );
    }
    while( std::count( placement.begin(), placement.end(), host ) && lst == rw );

    placement[chunkid] = host;

    return flags;
  }

  placement_t GeneratePlacement( const std::string      &objname,
                                 const placement_group  &plgr,
                                 bool                    write   )
  {
    Config &cfg = Config::Instance();

    static std::hash<std::string>  strhash;
    std::default_random_engine generator( strhash( objname ) );
    std::uniform_int_distribution<uint32_t>  distribution( 0, plgr.size() - 1 );

    placement_t placement;
    size_t      off = 0;

    while( placement.size() < cfg.nbchunks )
    {
      // check if the host is available
      auto &tpl = plgr[distribution( generator )];
      auto lst = std::get<1>( tpl );
      if( lst == LocationStatus::off ||
          ( write && lst != LocationStatus::rw ) )
      {
        ++off;
        if( plgr.size() - off < cfg.nbchunks ) throw std::exception(); // TODO
        continue;
      }
      // check if the host is already in our placement
      auto &host = std::get<0>( tpl );
      if( std::count( placement.begin(), placement.end(), host ) ) continue;
      // add new location
      placement.push_back( host );
    }

    return std::move( placement );
  }

  placement_t GetSpares( const placement_group &plgr, const placement_t &placement, bool write )
  {
    placement_t spares;
    spares.reserve( plgr.size() - placement.size() );

    for( auto &tpl : plgr )
    {
      auto &host = std::get<0>( tpl );
      if( std::count( placement.begin(), placement.end(), host ) ) continue;
      auto lst = std::get<1>( tpl );
      if( lst == LocationStatus::off || ( write && lst != LocationStatus::rw ) ) continue;
      spares.emplace_back( host );
    }

    return std::move( spares );
  }
}
