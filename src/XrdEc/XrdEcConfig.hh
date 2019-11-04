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

      RedundancyProvider redundancy;

      std::string        ckstype;

      uint8_t repairthreads;

      std::string headnode;

    private:

      Config() : blksize( 6144 ),
                 datasize( 4096 ),
                 paritysize( 2048 ),
                 chunksize( 512 ),
                 nbdata( 8 ),
                 nbparity( 4 ),
                 nbchunks( 12 ),
                 maxrelocate( 10 ),
                 redundancy( 8 /*nbdata*/, 4 /*nbparity*/ ),
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
