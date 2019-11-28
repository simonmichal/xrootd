/*
 * XrdEcObjCfg.hh
 *
 *  Created on: Nov 25, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECOBJCFG_HH_
#define SRC_XRDEC_XRDECOBJCFG_HH_

#include <string>
#include <stdlib.h>

namespace XrdEc
{
  struct ObjCfg
  {
      ObjCfg() = delete;

      ObjCfg( const std::string &obj ) : obj( obj ),
                                         nbchunks( ParseNbChunk( obj ) ),
                                         nbparity( ParseNbParity( obj ) ),
                                         nbdata( nbchunks - nbparity ),
                                         datasize( ParseDataSize( obj ) ),
                                         chunksize( datasize / nbdata ),
                                         paritysize( nbparity * chunksize ),
                                         blksize( datasize + paritysize )
      {
      }

      ObjCfg( const ObjCfg &objcfg ) : obj( objcfg.obj ),
                                       nbchunks( objcfg.nbchunks ),
                                       nbparity( objcfg.nbparity ),
                                       nbdata( objcfg.nbdata ),
                                       datasize( objcfg.datasize ),
                                       chunksize( objcfg.chunksize ),
                                       paritysize( objcfg.paritysize ),
                                       blksize( objcfg.blksize )
      {
      }

      ObjCfg( ObjCfg &&objcfg ) = delete;

      const std::string obj;
      const uint8_t     nbchunks;   // number of chunks in block
      const uint8_t     nbparity;   // number of chunks in parity
      const uint8_t     nbdata;     // number of chunks in data
      const uint64_t    datasize;   // size of the data in the block
      const uint64_t    chunksize;  // size of single chunk (nbchunks * chunksize = blksize)
      const uint64_t    paritysize; // size of the parity in the block
      const uint64_t    blksize;    // the whole block size (data + parity)

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
