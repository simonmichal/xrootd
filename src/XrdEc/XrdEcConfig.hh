/*
 * XrdEcConfig.hh
 *
 *  Created on: Dec 4, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECCONFIG_HH_
#define SRC_XRDEC_XRDECCONFIG_HH_

#include "XrdEc/XrdEcRedundancyProvider.hh"

#include <stdint.h>
#include <vector>
#include <string>

namespace XrdEc
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

      uint64_t blksize;    // the whole block size (data + parity)
      uint64_t datasize;   // size of the data in the block
      uint64_t paritysize; // size of the parity in the block
      uint64_t chunksize;  // size of single chunk (nbchunks * chunksize = blksize)
      uint8_t  nbdata;     // number of chunks in data
      uint8_t  nbparity;   // number of chunks in parity
      uint8_t  nbchunks;   // number of chunks in block

      uint8_t  maxrelocate;

      std::vector<std::string> hosts;

      RedundancyProvider redundancy;

      std::string        ckstype;

      uint8_t repairthreads;

    private:

      Config() : blksize( 1024 ),
                 datasize( 768 ),
                 paritysize( 256 ),
                 chunksize( 128 ),
                 nbdata( 6 ),
                 nbparity( 2 ),
                 nbchunks( 8 ),
                 maxrelocate( 10 ),
                 redundancy( 6 /*nbdata*/, 2 /*nbparity*/ ),
                 ckstype( "md5" ),
                 repairthreads( 4 )
      {
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
      }

      Config( const Config& ) = delete;

      Config( Config&& ) = delete;

      Config& operator=( const Config& ) = delete;

      Config& operator=( Config&& ) = delete;
  };
}


#endif /* SRC_XRDEC_XRDECCONFIG_HH_ */
