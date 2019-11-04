/*
 * XrdEcMetadataProvider.hh
 *
 *  Created on: Dec 14, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECMETADATAPROVIDER_HH_
#define SRC_XRDEC_XRDECMETADATAPROVIDER_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcConfig.hh"

#include <tuple>
#include <unordered_map>
#include <vector>

namespace XrdEc
{
  class MetadataProvider
  {
    public:

      static MetadataProvider& Instance()
      {
        static MetadataProvider mdp;
        return mdp;
      }

      virtual ~MetadataProvider()
      {

      }

      placement_group GetPlacementGroup( const std::string &path )
      {
        auto itr = pltables.find( path );
        if( itr == pltables.end() )
        {
          // create new placement
          static std::uniform_int_distribution<uint32_t>  distribution( 0, hosts.size() - 1 );
          static std::hash<std::string> strhash;
          std::default_random_engine generator( strhash( path ) );

          placement_group plgr;
          std::string host;
          do
          {
            host = hosts[distribution( generator ) ];
            if( !std::count( plgr.begin(), plgr.end(), host) )
              plgr.push_back( host );
          }
          while( plgr.size() != plgrsize );

          pltables[path] = std::move( plgr );
          return pltables[path];
        }

        return itr->second;
      }

      std::tuple<std::vector<uint64_t>, std::vector<placement_t>> GetPlacement( const std::string &path, const placement_group &plgr )
      {
        auto itr = placements.find( path );
        if( itr == placements.end() )
          return std::tuple<std::vector<uint64_t>, std::vector<placement_t>>();

        Config &cfg = Config::Instance();
        const size_t entrysize = sizeof( uint64_t ) + cfg.nbchunks;
        size_t nbblk = itr->second.size() / entrysize;

        std::vector<uint64_t> vers; vers.reserve( nbblk );
        std::vector<placement_t> pls; pls.reserve( nbblk );

        const char* raw = itr->second.c_str();
        for( size_t i = 0; i < nbblk; ++i )
        {
          uint64_t version = *reinterpret_cast<const uint64_t*>( raw );
          vers.emplace_back( version );
          raw += sizeof( uint64_t );

          placement_t pl; pl.reserve( cfg.nbchunks );
          for( size_t j = 0; j < cfg.nbchunks; ++j )
          {
            size_t index = raw[j];
            if( index >= plgr.size() ) throw std::exception(); // TODO
            pl.emplace_back( plgr[index] );
          }
          raw += cfg.nbchunks;
        }

        return std::make_tuple( std::move( vers ), std::move( pls ) );
      }

      void SetPlacement(const std::string &path, const placement_group &plgr, const std::vector<uint64_t> &version, const std::vector<placement_t> &placement )
      {
        if( version.size() != placement.size() ) throw std::exception(); // TODO

        Config &cfg = Config::Instance();
        const size_t entrysize = sizeof( uint64_t ) + cfg.nbchunks;
        std::string serialized;
        serialized.reserve( version.size() * entrysize );

        for( size_t i = 0; i < version.size(); ++i )
        {
          const char *ver = reinterpret_cast<const char*>( & version[i] );
          std::string verstr( ver, sizeof( uint64_t ) );
          serialized += verstr;
          for( auto itr = placement[i].begin(); itr != placement[i].end(); ++itr )
          {
            char index = GetIndex( *itr, plgr );
            serialized += index;
          }
        }

        placements[path] = std::move( serialized );
      }

//      void Set( const std::string &path, std::vector<uint64_t> &version, const std::vector<placement_t> &placement )
//      {
//        inmemory[path] = std::make_tuple( version, placement );
//      }
//
//      std::tuple<std::vector<uint64_t>, std::vector<placement_t>> Get( const std::string &path )
//      {
//        return inmemory[path];
//      }

    private:

      MetadataProvider() : plgrsize( 10 )
      {
        // hard coded for testing !!!
        hosts.push_back( "file://localhost/data/dir0" );
        hosts.push_back( "file://localhost/data/dir1" );
        hosts.push_back( "file://localhost/data/dir2" );
        hosts.push_back( "file://localhost/data/dir3" );
        hosts.push_back( "file://localhost/data/dir4" );
        hosts.push_back( "file://localhost/data/dir5" );
        hosts.push_back( "file://localhost/data/dir6" );
        hosts.push_back( "file://localhost/data/dir7" );
        hosts.push_back( "file://localhost/data/dir8" );
        hosts.push_back( "file://localhost/data/dir9" );
        hosts.push_back( "file://localhost/data/dir10" );
        hosts.push_back( "file://localhost/data/dir11" );
        hosts.push_back( "file://localhost/data/dir12" );
        hosts.push_back( "file://localhost/data/dir13" );
        hosts.push_back( "file://localhost/data/dir14" );
        hosts.push_back( "file://localhost/data/dir15" );
        hosts.push_back( "file://localhost/data/dir16" );
        hosts.push_back( "file://localhost/data/dir17" );
        hosts.push_back( "file://localhost/data/dir18" );
        hosts.push_back( "file://localhost/data/dir19" );
        hosts.push_back( "file://localhost/data/dir20" );
        hosts.push_back( "file://localhost/data/dir21" );
        hosts.push_back( "file://localhost/data/dir22" );
        hosts.push_back( "file://localhost/data/dir23" );
        hosts.push_back( "file://localhost/data/dir24" );
        hosts.push_back( "file://localhost/data/dir25" );
        hosts.push_back( "file://localhost/data/dir26" );
        hosts.push_back( "file://localhost/data/dir27" );
        hosts.push_back( "file://localhost/data/dir28" );
        hosts.push_back( "file://localhost/data/dir29" );
      }

      MetadataProvider( const MetadataProvider& ) = delete;

      MetadataProvider( MetadataProvider&& ) = delete;

      MetadataProvider& operator=( const MetadataProvider& ) = delete;

      MetadataProvider& operator=( MetadataProvider&& ) = delete;

      static char GetIndex( const std::string &pl, const placement_group &plgr )
      {
        char size = plgr.size();
        for( char ret = 0; ret < size; ++ret )
          if( plgr[ret] == pl ) return ret;
        throw std::exception(); // TODO
      }

//      std::unordered_map<std::string, std::tuple<std::vector<uint64_t>, std::vector<placement_t>>> inmemory;

      std::vector<std::string> hosts;

      size_t plgrsize;

      std::unordered_map<std::string, placement_group> pltables;

      std::unordered_map<std::string, std::string> placements; // <path, serialized placement>
                                                               // serialization format:
                                                               //   1. placememnt entry: 8bytes (version, uint64_t) + 1bytes x nbchunks
                                                               //   2. no delimiter between entries

  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECMETADATAPROVIDER_HH_ */
