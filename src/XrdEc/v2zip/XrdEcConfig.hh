/*
 * XrdEcConfig.hh
 *
 *  Created on: Dec 4, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECCONFIG_HH_
#define SRC_XRDEC_XRDECCONFIG_HH_

#include "XrdEc/v2zip/XrdEcRedundancyProvider.hh"
#include "XrdEc/v2zip/XrdEcObjCfg.hh"

#include <stdint.h>
#include <string>
#include <unordered_map>

namespace XrdEc2
{
  class Config
  {
    public:

      static Config& Instance()
      {
        static Config config;
        return config;
      }

      ~Config()
      {
      }

      RedundancyProvider& GetRedundancy( const ObjCfg &objcfg )
      {
        std::string key;
        key += std::to_string( objcfg.nbchunks );
        key += ':';
        key += std::to_string( objcfg.nbparity );
        key += '-';
        key += std::to_string( uint8_t( objcfg.datasize / ObjCfg::_1MB ) );

        auto itr = redundancies.find( key );
        if( itr == redundancies.end() )
        {
          auto p = redundancies.emplace( key, objcfg );
          return p.first->second;
        }
        else
          return itr->second;
      }

      uint8_t            maxrelocate;
      std::string        ckstype;
      uint8_t            repairthreads;
      std::string        headnode;

    private:

      std::unordered_map<std::string, RedundancyProvider> redundancies;

      Config() : maxrelocate( 10 ),
                 ckstype( "crc32" ),
                 repairthreads( 4 ),
                 headnode( "eospps.cern.ch" )
      {
      }

      Config( const Config& ) = delete;

      Config( Config&& ) = delete;

      Config& operator=( const Config& ) = delete;

      Config& operator=( Config&& ) = delete;
  };
}


#endif /* SRC_XRDEC_XRDECCONFIG_HH_ */
