/*
 * PlacementHandler.h
 *
 *  Created on: 8 May 2020
 *      Author: simonm
 */

#ifndef SRC_API_TEST_PLACEMENTHANDLER_HH_
#define SRC_API_TEST_PLACEMENTHANDLER_HH_

#include <stdint.h>

#include <vector>
#include <queue>
#include <mutex>

class PlacementHandler
{
  public:
    PlacementHandler( uint8_t srvnb ) : placement( AllocDisks( srvnb ) )
    {
    }

    virtual ~PlacementHandler()
    {
    }

    std::vector<std::string> GetPlacement()
    {
      std::unique_lock<std::mutex> lck( mtx );
      std::vector<std::string> ret = std::move( placement.front() );
      placement.pop();
      placement.push( ret );
      return std::move( ret );
    }

  private:

    typedef std::vector<std::string> placement_t;

    //-------------------------------------------------------------------------
    // Allocate disks to one of 4 servers
    //-------------------------------------------------------------------------
    static std::queue<placement_t> AllocDisks( uint8_t srvnb );

    //-------------------------------------------------------------------------
    // Disks allocated to this server
    //-------------------------------------------------------------------------
    std::queue<placement_t> placement;

    //-------------------------------------------------------------------------
    // Mutex guarding the instance
    //-------------------------------------------------------------------------
    mutable std::mutex      mtx;

};

#endif /* SRC_API_TEST_PLACEMENTHANDLER_HH_ */
