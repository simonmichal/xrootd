/*
 * XrdEcMetadataProvider.hh
 *
 *  Created on: Dec 14, 2018
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECMETADATAPROVIDER_HH_
#define SRC_XRDEC_XRDECMETADATAPROVIDER_HH_

#include "XrdEc/XrdEcUtilities.hh"

#include <tuple>
#include <unordered_map>
#include <vector>

namespace XrdEc
{
  class MetadataProvider
  {
    public:

      MetadataProvider()
      {

      }

      virtual ~MetadataProvider()
      {

      }

      void Set( const std::string &path, std::vector<uint64_t> &version, const std::vector<placement_t> &placement )
      {
        inmemory[path] = std::make_tuple( version, placement );
      }

      std::tuple<std::vector<uint64_t>, std::vector<placement_t>> Get( const std::string &path )
      {
        return inmemory[path];
      }

    private:

      std::unordered_map<std::string, std::tuple<std::vector<uint64_t>, std::vector<placement_t>>> inmemory;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECMETADATAPROVIDER_HH_ */
