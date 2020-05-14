/*
 * StreamSimulator.cc
 *
 *  Created on: 8 May 2020
 *      Author: simonm
 */

#include "StreamSimulator.hh"
#include "XrdEc/XrdEcDataObject.hh"
#include "XrdCl/XrdClMessageUtils.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClDefaultEnv.hh"

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cmath>
#include <iostream>
#include <fstream>


std::atomic<uint64_t>  StreamSimulator::cpcnt;
std::atomic<uint64_t>  StreamSimulator::cumulative_throughput_mb;
std::atomic<long long> StreamSimulator::max_duration;
std::atomic<uint64_t>  StreamSimulator::stddev_sum;
std::atomic<uint64_t>  StreamSimulator::errcnt;
std::atomic<uint64_t>  StreamSimulator::exceeded40sec;
long long              StreamSimulator::start_timestamp = StreamSimulator::time_nsec().count();


namespace
{
  std::string get_hostname()
  {
    char name[1024];
    if( gethostname( name, 1024 ) )
    {
      std::cout << "Failed to get the hostname!" << std::endl;
      throw std::exception();
    }
    return name;
  }
}


StreamSimulator::StreamSimulator( PlacementHandler &plhandler, uint8_t srvnb, uint8_t strmnb ) : srvnb( srvnb ), strmnb( strmnb ), objcnt( 1 ), hostname( get_hostname() ), plhandler( plhandler )
{
}

//------------------------------------------------------------------------------
// Copy 2GB which corresponds to TF
//------------------------------------------------------------------------------
bool StreamSimulator::copy_2GB( const XrdEc::ObjCfg &objcfg, uint64_t cpnb )
{
  XrdCl::Log *log = XrdCl::DefaultEnv::GetLog();
  XrdEc::DataObject obj;

  // open the object
  log->Info( XrdCl::UtilityMsg, "Opening %s.", objcfg.obj.c_str() );
  XrdCl::SyncResponseHandler h1;
  obj.Open( objcfg, XrdEc::StrmWrtMode, &h1 );
  h1.WaitForResponse();
  std::unique_ptr<XrdCl::XRootDStatus> s1( h1.GetStatus() );
  if( !s1->IsOK() )
  {
    std::cout << "Open failed : " << s1->ToString() << std::endl;
    ++errcnt;
    return false;
  }

  // write 2GB, we write
  log->Info( XrdCl::UtilityMsg, "Writing to %s.", objcfg.obj.c_str() );
  uint32_t left_to_wrt = _2GB;
  uint64_t offset = 0;
  while( left_to_wrt > 0 )
  {
    uint32_t size = _10MB;
    if( size > left_to_wrt )
      size = left_to_wrt;

    XrdCl::SyncResponseHandler h2;
    obj.Write( offset, size, get_data(), &h2 );
    h2.WaitForResponse();
    std::unique_ptr<XrdCl::XRootDStatus> s2( h2.GetStatus() );
    if( !s2->IsOK() )
    {
      std::cout << "Write failed : " << s2->ToString() << std::endl;
      ++errcnt;
      return false;
    }

    offset      += size;
    left_to_wrt -= size;
  }

  // close the object
  log->Info( XrdCl::UtilityMsg, "Closing %s.", objcfg.obj.c_str() );
  XrdCl::SyncResponseHandler h3;
  obj.Close( &h3 );
  h3.WaitForResponse();
  std::unique_ptr<XrdCl::XRootDStatus> s3( h3.GetStatus() );
  if( !s3->IsOK() )
  {
    std::cout << "Close failed : " << s3->ToString() << std::endl;
    ++errcnt;
    return false;
  }

  return true;
}

bool StreamSimulator::copy_2GB_fork( const XrdEc::ObjCfg &objcfg, uint64_t cpnb )
{
  XrdCl::Log *log = XrdCl::DefaultEnv::GetLog();
  XrdEc::DataObject obj;



  pid_t pid = fork();

  if( pid == 0 )
  {
    //child
    std::string logfn = "xrdcl.stream." + std::to_string( cpnb ) + ".txt";
    XrdCl::DefaultEnv::SetLogLevel( "Dump" );
    XrdCl::DefaultEnv::SetLogFile( logfn );

    // open the object
    log->Info( XrdCl::UtilityMsg, "Opening %s.", objcfg.obj.c_str() );
    XrdCl::SyncResponseHandler h1;
    obj.Open( objcfg, XrdEc::StrmWrtMode, &h1 );
    h1.WaitForResponse();
    std::unique_ptr<XrdCl::XRootDStatus> s1( h1.GetStatus() );
    if( !s1->IsOK() )
    {
      std::cout << "Open failed : " << s1->ToString() << " (cpnb=" <<cpnb << ")" << std::endl;
      exit( 1 );
    }

    // write 2GB, we write
    log->Info( XrdCl::UtilityMsg, "Writing to %s.", objcfg.obj.c_str() );
    uint32_t left_to_wrt = _2GB;
    uint64_t offset = 0;
    while( left_to_wrt > 0 )
    {
      uint32_t size = _10MB;
      if( size > left_to_wrt )
        size = left_to_wrt;

      XrdCl::SyncResponseHandler h2;
      obj.Write( offset, size, get_data(), &h2 );
      h2.WaitForResponse();
      std::unique_ptr<XrdCl::XRootDStatus> s2( h2.GetStatus() );
      if( !s2->IsOK() )
      {
        std::cout << "Write failed : " << s2->ToString() << " (cpnb=" <<cpnb << ")" << std::endl;
        exit( 1 );
      }

      offset      += size;
      left_to_wrt -= size;
    }

    // close the object
    log->Info( XrdCl::UtilityMsg, "Closing %s.", objcfg.obj.c_str() );
    XrdCl::SyncResponseHandler h3;
    obj.Close( &h3 );
    h3.WaitForResponse();
    std::unique_ptr<XrdCl::XRootDStatus> s3( h3.GetStatus() );
    if( !s3->IsOK() )
    {
      std::cout << "Close failed : " << s3->ToString() << " (cpnb=" <<cpnb << ")" << std::endl;
      exit( 1 );
    }

    exit( 0 );
  }
  else if( pid < 0 )
  {
    ++errcnt;
    std::cout << "Failed to fork new process!" << std::endl;
    return false;
  }
  else
  {
    // parent
    while( true )
    {
      int status;
      pid_t w = waitpid( pid, &status, 0 );
      if( w != pid ) continue;

      if( WIFEXITED(status) )
      {
        if( WEXITSTATUS(status) )
        {
          ++errcnt;
          return false;
        }

        return true;
      }

      if( WIFSIGNALED(status) )
      {
        ++errcnt;
        return false;
      }
    }
  }
}

//------------------------------------------------------------------------------
// Simulate a TF stream
//------------------------------------------------------------------------------
void StreamSimulator::simulate_stream( long long shift )
{
  std::string logfn = "stream." + std::to_string( shift ) + ".txt";
  std::fstream fout( logfn, std::fstream::out );

  synchronize_stream( shift );

  while( true )
  {
    uint64_t cpnb = cpcnt.fetch_add( 1 );
    auto start = time_nsec();
    XrdEc::ObjCfg objcfg( get_obj_name(), "0001", 10, 2, _1MB );
    objcfg.plgr = plhandler.GetPlacement();
    bool success = copy_2GB_fork( objcfg, cpnb );
    auto stop = time_nsec();

    auto duration_nsec = stop.count() - start.count();
    if( duration_nsec > to_nsec( 40 ) )
    {
      std::cout << "2GB copy took too long: " << cpnb << std::endl;
      ++exceeded40sec;
    }
    else
      std::cout << "Copied 2GB in " << ( duration_nsec / 1000000 ) << " ms." << std::endl;



    double duration_ms = duration_nsec / 1000000.0;
    double duration_s  = duration_ms   / 1000.0;
    double dta = 2 * 1024 /*2048 MB*/ / duration_s;
    uint64_t running_for = ( time_nsec().count() - start_timestamp ) / 1000000000;

    cumulative_throughput_mb.fetch_add( uint64_t( dta ) );

    if( duration_nsec > max_duration.load() ) max_duration.store( duration_nsec );
    double avg = cumulative_throughput_mb.load() / double( cpcnt.load() );
    stddev_sum += ( dta - avg ) * ( dta - avg );
    double stddev = sqrt( stddev_sum / cpcnt );

    std::cout << "Average throughput : " << avg << " MB/s." << std::endl;
    std::cout << "Standard deviation : " << stddev << std::endl;
    std::cout << "Max duration       : " << ( max_duration.load() / 1000000.0 ) << " ms." << std::endl;
    std::cout << "Error count        : " << errcnt.load() << std::endl;
    std::cout << "Exceeded 40s count : " << exceeded40sec.load() << std::endl;
    std::cout << "Running for        : " << running_for << " seconds." << std::endl;

    fout << "Copied " << cpnb << " TF  : \n";
    fout << "Data transfer rate : " << dta <<  " MB/s\n";
    fout << "Duration           : " << duration_ms   << " ms.\n";
    fout << "Error count        : " << errcnt        << '\n';
    fout << "Exceeded 40s count : " << exceeded40sec << '\n';
    fout << "Running for        : " << running_for << " seconds.\n";
    if( duration_s > 40 )
      fout << "Duration exceeded 40s.\n";
    if( !success )
      fout << "Transfer failed due to an error!\n";
    fout << std::endl;
    fout.flush();

    // recalculate duration after updating statistics
    stop = time_nsec();
    duration_nsec = stop.count() - start.count();

    wait_nsec( to_nsec( 40 ) - duration_nsec );
  }

  fout.close();
}

//------------------------------------------------------------------------------
// Get the name for the next TF to be written
//------------------------------------------------------------------------------
std::string StreamSimulator::get_obj_name()
{
  std::unique_lock<std::mutex> lck( mtx );
  std::stringstream ss;
  ss << std::setfill( '0' ) << std::setw( 10 ) << objcnt;
  ++objcnt;
  std::string ret = hostname + '.' + std::to_string( strmnb ) + '.' + ss.str();
  return std::move( ret );
}
