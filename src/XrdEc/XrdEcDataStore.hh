/*
 * XrdEcStore.hh
 *
 *  Created on: Dec 3, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECDATASTORE_HH_
#define SRC_XRDEC_XRDECDATASTORE_HH_

#include "XrdEc/XrdEcBlock.hh"
#include "XrdEc/XrdEcBlockPool.hh"
#include "XrdEc/XrdEcStripeReader.hh"
#include "XrdEc/XrdEcMetadataProvider.hh"

#include <XrdCl/XrdClXRootDResponses.hh>

#include <string>
#include <vector>
#include <memory>
#include <future>

namespace XrdEc
{

  class DataStore
  {
    public:

      DataStore( const std::string &path ) : path( path )
      {
        std::tie( version, placement ) = metadata.Get( path );
      }

      virtual ~DataStore()
      {
        metadata.Set( path, version, placement );
      }

      uint64_t Read( uint64_t offset, uint64_t size, void *buffer )
      {
        char *buff = reinterpret_cast<char*>( buffer );
        StripeReader reader( path, placement, version );
        return reader.Read( offset, size, buff );
      }

      void Write( uint64_t offset, uint64_t size, const void *buffer )
      {
        if( !wrtcache )
        {
          wrtcache.reset( new Block( BlockPool::Create() ) );
          wrtcache->Reset( path, offset, placement, version ); // TODO if we are going to overwrite the whole block there is no need to pre-load the old value
        }

        const char *buff = reinterpret_cast<const char*>( buffer );

        while( size > 0 )
        {
          size_t written = wrtcache->Write( offset, size, buff );

          offset += written;
          buff   += written;

          if( written < size )
          {
            wrtcache->Sync();
            wrtcache->Update( version, placement );
            wrtcache->Reset( path, offset, placement, version ); // TODO if we are going to overwrite the whole block there is no need to pre-load the old value
          }

          size -= written;
        }
      }

      inline void Sync()
      {
        wrtcache->Sync();
        wrtcache->Update( version, placement );
        wrtcache.reset();
      }

      void Truncate( uint64_t size ) // can be used to wipe out the data
      {
        Config &cfg = Config::Instance();

        uint64_t lstblk  = size % cfg.datasize;
        uint64_t fullblk = size / cfg.datasize;
        uint64_t nbblk   = fullblk + ( lstblk ? 1 : 0 );

        if( nbblk > placement.size() )
        {
          TruncateBlock( placement.size() - 1, cfg.datasize );
          AddEmptyBlocks( fullblk, lstblk );
        }
        else // if( nbblk <= placement.size() )
        {
          RemoveBlocks( nbblk );
          if( !lstblk ) lstblk = cfg.datasize;
          TruncateBlock( nbblk - 1, lstblk );
        }
      }

      void Finalize();

    private:

      void RemoveBlocks( uint64_t nbblk );

      void TruncateBlock( uint64_t blkid, uint64_t blksize );

      void AddEmptyBlocks( uint64_t fullblk, uint64_t lstblk );

      std::string              path;

    public:
      std::vector<uint64_t>    version;
      std::vector<placement_t> placement;

    private:
      std::unique_ptr<Block>   wrtcache;
      MetadataProvider         metadata;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECDATASTORE_HH_ */
