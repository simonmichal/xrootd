/*
 * StreamSimulator.hh
 *
 *  Created on: 8 May 2020
 *      Author: simonm
 */

#ifndef SRC_API_TEST_STREAMSIMULATOR_HH_
#define SRC_API_TEST_STREAMSIMULATOR_HH_

#include "XrdEc/XrdEcObjCfg.hh"
#include "PlacementHandler.hh"

#include <stdint.h>

#include <chrono>
#include <thread>
#include <sstream>
#include <mutex>
#include <iomanip>
#include <iostream>
#include <atomic>


class StreamSimulator
{
  public:
    StreamSimulator( PlacementHandler &plhandler, uint8_t srvnb, uint8_t strmnb );

    virtual ~StreamSimulator()
    {
    }

    //------------------------------------------------------------------------------
    // srvnb  : 1, 2, 3 or 4
    // strmnb : 0, 1, 2, .., 49
    //------------------------------------------------------------------------------
    void Start()
    {
      long long shift = srvnb * 200;
      shift += strmnb * 800;
      simulate_stream( shift );
    }

  private:

    //------------------------------------------------------------------------------
    // Simulate a TF stream
    //------------------------------------------------------------------------------
    void simulate_stream( long long shift );

    //------------------------------------------------------------------------------
    // Get current time in nanoseconds
    //------------------------------------------------------------------------------
    static inline std::chrono::nanoseconds time_nsec()
    {
      using namespace std::chrono;
      auto since_epoch = high_resolution_clock::now().time_since_epoch();
      return duration_cast<nanoseconds>( since_epoch );
    }

    //------------------------------------------------------------------------------
    // Convert seconds to nanoseconds
    //------------------------------------------------------------------------------
    inline long long to_nsec( long long sec )
    {
      return sec * 1000000000;
    }

    //------------------------------------------------------------------------------
    // Make sure the streams starts yielding data at the right moment
    //------------------------------------------------------------------------------
    inline void synchronize_stream( long long shift )
    {
      using namespace std::chrono;
      auto since_epoch = high_resolution_clock::now().time_since_epoch();
      auto nsec = 1000000000 - duration_cast<nanoseconds>( since_epoch ).count() % 1000000000;
      // synchronize to full second and shift by requested value
      auto nsec_to_wait = to_nsec( 59 ) + nsec + 1000000 * shift;
      std::cout << "synchronizing, wait for : " << nsec_to_wait << " nano seconds!" << std::endl;
      std::cout << "(" << (nsec_to_wait / 1000000 ) << " ms)" << std::endl;
      std::this_thread::sleep_for( nanoseconds( nsec_to_wait ) );
      std::cout << "we should be in sync now" << std::endl;

      start_timestamp = high_resolution_clock::now().time_since_epoch().count();
    }

    //------------------------------------------------------------------------------
    // Wait for number of nano-seconds
    //------------------------------------------------------------------------------
    inline void wait_nsec( long long nsec )
    {
      using namespace std::chrono;
      std::this_thread::sleep_for( nanoseconds( nsec ) );
    }

    static const size_t _1MB  = 1024 * 1024;
    static const size_t _10MB = 10 * _1MB;
    static const size_t _2GB  = 2 * 1024 * _1MB;

    uint8_t             srvnb;
    uint8_t             strmnb;

    mutable std::mutex  mtx;
    size_t              objcnt;

    std::string         hostname;

    PlacementHandler   &plhandler;

    //------------------------------------------------------------------------------
    // Get a data block for writing
    //------------------------------------------------------------------------------
    static const char* get_data()
    {
      static std::string *data = new std::string( _10MB, 'A' );
      return data->c_str();
    }

    std::string get_obj_name();

    void copy_2GB( const XrdEc::ObjCfg &objcfg );
    void copy_2GB_fork( const XrdEc::ObjCfg &objcfg );

    static std::atomic<uint64_t>  cpcnt;
    static std::atomic<uint64_t>  cumulative_throughput_mb;
    static std::atomic<long long> max_duration;
    static std::atomic<uint64_t>  stddev_sum;
    static std::atomic<uint64_t>  errcnt;
    static std::atomic<uint64_t>  exceeded40sec;
    static long long start_timestamp;
};

#endif /* SRC_API_TEST_STREAMSIMULATOR_HH_ */
