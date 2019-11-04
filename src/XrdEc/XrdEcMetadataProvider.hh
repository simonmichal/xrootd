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
#include <sstream>

#include "qclient/QSet.hh"
#include "qclient/AsyncHandler.hh"

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

//      placement_group GetPlacementGroup( const std::string &path )
//      {
//        placement_group plgr = QDBGetPlGr( path );
//        if( plgr.empty() )
//        {
//          std::vector<std::string> locations( QDBGetLocations() );
//          // create new placement
//          static std::uniform_int_distribution<uint32_t>  distribution( 0, locations.size() - 1 );
//          static std::hash<std::string> strhash;
//          std::default_random_engine generator( strhash( path ) );
//
//          std::string location;
//          do
//          {
//            location = locations[distribution( generator ) ];
//            if( !std::count( plgr.begin(), plgr.end(), location) )
//              plgr.push_back( location );
//          }
//          while( plgr.size() != plgrsize );
//
//          // persist in QuarkDB
//          QDBSetPlGr( path, plgr );
//        }
//
//        return std::move( plgr );
//      }

      std::tuple<std::vector<uint64_t>, std::vector<placement_t>> GetPlacement( const std::string &path, const placement_group &plgr )
      {
        std::string placement = QDBGetPlacement( path );

        if( placement.empty() )
          return std::tuple<std::vector<uint64_t>, std::vector<placement_t>>();

        Config &cfg = Config::Instance();
        const size_t entrysize = sizeof( uint64_t ) + cfg.nbchunks;
        size_t nbblk = placement.size() / entrysize;

        std::vector<uint64_t> vers; vers.reserve( nbblk );
        std::vector<placement_t> pls; pls.reserve( nbblk );

        const char* raw = placement.c_str();
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

        QDBSetPlacement( path, serialized );
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

      std::vector<std::string> QDBGetLocations()
      {
        static const std::string key = "xrdec.locations";

        qclient::redisReplyPtr reply = qcl.exec( "get", key ).get();

        if( reply == nullptr )
          throw std::exception(); // TODO

        if(reply->type != REDIS_REPLY_STRING )
          throw std::exception(); // TODO

        if( reply->len <= 0 )
          throw std::exception(); // TODO

        std::istringstream iss( reply->str );
        std::vector<std::string> ret;
        std::string location;

        while( std::getline( iss, location, '\n' ) )
          ret.push_back( location );

        return std::move( ret );
      }

//      placement_group QDBGetPlGr( const std::string &path )
//      {
//        static const std::string prefix = "xrdec.plgr.";
//
//        qclient::redisReplyPtr reply = qcl.exec( "get", prefix + path ).get();
//
//        if( reply == nullptr )
//          throw std::exception(); // TODO
//
//        if( reply->type == REDIS_REPLY_NIL )
//          return placement_group();
//
//        if(reply->type != REDIS_REPLY_STRING )
//          throw std::exception(); // TODO
//
//        if( reply->len < 0 )
//          throw std::exception(); // TODO
//
//        if( reply->len == 0 )
//          return placement_group();
//
//        std::istringstream iss( reply->str );
//        placement_group ret;
//        std::string location;
//
//        while( std::getline( iss, location, '\n' ) )
//          ret.push_back( location );
//
//        return std::move( ret );
//      }

      void QDBSetPlGr( const std::string &path, const placement_group &plgr )
      {
        static const std::string prefix = "xrdec.plgr.";

        std::string strplgr;
        for( auto &location : plgr )
        {
          strplgr += location + '\n';
        }

        qclient::redisReplyPtr reply = qcl.exec( "set", prefix + path, strplgr ).get();

        if( reply == nullptr )
          throw std::exception(); // TODO

        if(reply->type != REDIS_REPLY_STATUS )
          throw std::exception(); // TODO

        if( reply->len <= 0 )
          throw std::exception(); // TODO

        if( strcmp( "OK", reply->str ) != 0 )
          throw std::exception(); // TODO
      }

      std::string QDBGetPlacement( const std::string &path )
      {
        static const std::string prefix = "xrdec.pl.";

        qclient::redisReplyPtr reply = qcl.exec( "get", prefix + path ).get();

        if( reply == nullptr )
          throw std::exception(); // TODO

        if( reply->type == REDIS_REPLY_NIL )
          return std::string();

        if(reply->type != REDIS_REPLY_STRING )
          throw std::exception(); // TODO

        if( reply->len < 0 )
          throw std::exception(); // TODO

        if( reply->len == 0 )
          return std::string();

        return reply->str;
      }

      void QDBSetPlacement(const std::string &path, const std::string &placement )
      {
        static const std::string prefix = "xrdec.pl.";

        qclient::redisReplyPtr reply = qcl.exec( "set", prefix + path, placement ).get();

        if( reply == nullptr )
          throw std::exception(); // TODO

        if(reply->type != REDIS_REPLY_STATUS )
          throw std::exception(); // TODO

        if( reply->len <= 0 )
          throw std::exception(); // TODO

        if( strcmp( "OK", reply->str ) != 0 )
          throw std::exception(); // TODO
      }


      MetadataProvider() : plgrsize( 10 ), qcl{ "quarkdb-test", 7777, {} } // we should load this from a config file TODO
      {
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

      size_t plgrsize;

//      std::unordered_map<std::string, std::string> placements; // <path, serialized placement>
                                                               // serialization format:
                                                               //   1. placememnt entry: 8bytes (version, uint64_t) + 1bytes x nbchunks
                                                               //   2. no delimiter between entries

      qclient::QClient qcl;

  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECMETADATAPROVIDER_HH_ */
