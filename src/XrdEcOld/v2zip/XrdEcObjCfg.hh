/*
 * XrdEcObjCfg.hh
 *
 *  Created on: Nov 25, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECOBJCFG_HH_
#define SRC_XRDEC_XRDECOBJCFG_HH_

#include <cstdlib>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

namespace XrdEc2
{
  struct ObjCfg
  {
      ObjCfg() = delete;

      ObjCfg( const std::string &obj ) : obj( obj ),
                                         nbchunks( 12 ),
                                         nbparity( 4 ),
                                         nbdata( 8 ),
                                         datasize( 32 * _1MB ),
                                         chunksize( 4 * _1MB ),
                                         paritysize( 16 * _1MB ),
                                         blksize( 48 * _1MB)
      {
        plgr.reserve( 16 );
        for( size_t i = 0; i < 16; ++i )
        {
          std::stringstream strm;
          strm << "file://localhost/data/v2/dir" << std::setw( 2 ) << std::setfill( '0' ) << i << '/';
          plgr.emplace_back( strm.str() );
        }
      }

      ObjCfg( const ObjCfg &objcfg ) : obj( objcfg.obj ),
                                       nbchunks( objcfg.nbchunks ),
                                       nbparity( objcfg.nbparity ),
                                       nbdata( objcfg.nbdata ),
                                       datasize( objcfg.datasize ),
                                       chunksize( objcfg.chunksize ),
                                       paritysize( objcfg.paritysize ),
                                       blksize( objcfg.blksize ),
                                       plgr( objcfg.plgr )
      {
      }

      const std::string obj;
      const uint8_t     nbchunks;   // number of chunks in block
      const uint8_t     nbparity;   // number of chunks in parity
      const uint8_t     nbdata;     // number of chunks in data
      const uint64_t    datasize;   // size of the data in the block
      const uint64_t    chunksize;  // size of single chunk (nbchunks * chunksize = blksize)
      const uint64_t    paritysize; // size of the parity in the block
      const uint64_t    blksize;    // the whole block size (data + parity) in MB

      std::vector<std::string> plgr;

      static const uint64_t _1MB = 1024; // TODO * 1024;

    private:

      inline static uint8_t ParseNbChunk( const std::string &objname )
      {
        size_t pos = objname.find( '-' );   // EC algorithm
        pos = objname.find( '-', pos + 1 ); // EC number data
        size_t begin = pos + 1;

        char *endptr;
        return std::strtoul( objname.c_str() + begin, &endptr, 10 );
      }

      inline static uint8_t ParseNbParity( const std::string &objname )
      {
        size_t pos = objname.find( ':' );   // EC number parity
        size_t begin = pos + 1;

        char *endptr;
        return std::strtoul( objname.c_str() + begin, &endptr, 10 );
      }

      inline static uint64_t ParseDataSize( const std::string &objname )
      {
        size_t pos = objname.find( '-' );   // EC algorithm
        pos = objname.find( '-', pos + 1 ); // EC parameters
        pos = objname.find( '-', pos + 1 ); // Block size in MB (begin)
        size_t begin = pos + 1;

        char *endptr;
        return std::strtoull( objname.c_str() + begin, &endptr, 10 ) * _1MB;
      }
  };
}


#endif /* SRC_XRDEC_XRDECOBJCFG_HH_ */
