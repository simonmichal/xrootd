#include <thread>
#include <iostream>
#include <sstream>
#include <condition_variable>
#include <mutex>

#include <string.h>

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"
#include "XrdCl/XrdClEnv.hh"
#include "XrdCl/XrdClDefaultEnv.hh"

const size_t _10MB =  1024 * 1024 * 10;

void wrt_data( std::string url, char* buffer )
{
  std::cout << url << std::endl;

  XrdCl::File file;
  XrdCl::XRootDStatus st = file.Open( url, XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write );
  if( !st.IsOK() )
  {
    std::cout << "Open failed: " << st.ToString() << std::endl;
    return;
  }

  std::mutex mtx;
  std::condition_variable cv;
  size_t ongoing = 0;
  const size_t treshold = 32;
  size_t chunks_left = 1024;
  uint64_t offset = 0;

   std::unique_lock<std::mutex> lck( mtx );

  // spawn transfers
  while( chunks_left > 0 )
  {
    if( ongoing < treshold )
    {
      XrdCl::Async( XrdCl::Write( file, offset, _10MB, buffer ) >> [&]( XrdCl::XRootDStatus &st )
          {
            if( !st.IsOK() ) abort();
            std::unique_lock<std::mutex> lck( mtx );
            --ongoing;
            cv.notify_all();
          } );
      ++ongoing;
      --chunks_left;
      offset += _10MB;
    }
    else
      cv.wait( lck );
  }

  // wait for ongoing transfers
  while( ongoing > 0 ) cv.wait( lck );

  st = file.Close();
  if( !st.IsOK() )
  {
    std::cout << "Close failed: " << st.ToString() << std::endl;
    return;
  }
}


int main( int argc, char **argv )
{
  std::cout << "There we go ..." << std::endl;
 
  XrdCl::Env *env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt( "ParallelEvtLoop", 5 );
 
  char *buffer = new char[_10MB];
  memset( buffer, 'A', _10MB );

  int port = 2094;
  std::vector<std::thread> threads; threads.reserve( 96 );

//  std::string arg = argc > 1 ? argv[1] : "hba0";
//
//  size_t hbanb = arg == "hba0" ? 0 : 
//                 arg == "hba1" ? 1 :
//                 arg == "hba2" ? 2 : 3;

  for( size_t hbanb = 0; hbanb < 4; ++hbanb )
  {
    for( size_t dirnb = 0; dirnb < 24; ++dirnb )
    {
      std::stringstream ss;
      ss << "root://hba" << hbanb << '-' << dirnb << "@st-096-100gb001:" << ( port + hbanb );
      ss << "//hba" << hbanb << "data/data" << dirnb << "/xrdtest/remote.dat";
      std::string url = ss.str();
      threads.emplace_back( wrt_data, url, buffer );
    }
  }

  for( auto &t : threads )
    t.join(); 

  delete[] buffer;

  std::cout << "The End." << std::endl;
  return 0;
}
