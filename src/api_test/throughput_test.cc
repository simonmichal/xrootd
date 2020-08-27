/*
 * xrdec2_test.cc
 *
 *  Created on: Jan 27, 2020
 *      Author: simonm
 */




#include "XrdEc/XrdEcDataObject.hh"
#include "XrdCl/XrdClMessageUtils.hh"

#include <iostream>
#include <fstream>
#include <thread>
#include <sstream>
#include <future>
#include <vector>
#include <queue>

const size_t nbmb = 16;

void Copy10GB( size_t hbanb, size_t disknb, char *buffer, size_t length )
{
  std::stringstream ss;
  ss << "root://";                                                      // protocol
  ss << "user-" << hbanb << '-' << disknb << '@';                       // user
  ss << "st-096-100gb001:";                                             // host
  ss << ( 2094 + hbanb );                                               // port
  ss << "//hba" << hbanb << "data/data" << disknb << "/xrdtest/manya.dat";  // path
  std::string url = ss.str();

  std::cout << "Transferring to : " << url << std::endl;

  XrdCl::File f;
  auto st = f.Open( url, XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write );
  if( !st.IsOK() )
  {
    std::cout << st.ToString() << std::endl;
    exit( 1 );
  }

  size_t iterations = ( 1024 / nbmb ) * 10;
  uint64_t offset = 0;
  const size_t infly = 16;
  std::queue<std::future<XrdCl::XRootDStatus>> ftrs;
  while( iterations > 0 )
  {
    if( ftrs.size() >= infly )
    {
      auto status = ftrs.front().get();
      if( !status.IsOK() )
      {
        std::cout << status.ToString() << std::endl;
	exit( 1 );
      }
      ftrs.pop();
    }

    ftrs.emplace( XrdCl::Async( XrdCl::Write( f, offset, length, buffer ) ) );
    offset += length;
    --iterations;
  }

  while( ftrs.size() > 0 )
  {
    auto status = ftrs.front().get();
    if( !status.IsOK() )
    {
      std::cout << status.ToString() << std::endl;
      exit( 1 );
    }
    ftrs.pop();
  }


  st = f.Close();
  if( !st.IsOK() )
  {
    std::cout << st.ToString() << std::endl;
    exit( 1 );
  }
}


int main( int argc, char **argv )
{
  std::cout << "There we go!" << std::endl;
  
  XrdCl::Env *env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt( "ParallelEvtLoop", 4 );

  size_t  length = 1024 * 1024 * nbmb; // 10MB;
  char   *buffer = new char[length];
  memset( buffer, 'a', length );

  std::vector<std::thread> workers;
  for( size_t hbanb = 0; hbanb < 4; ++hbanb )
  {
    for( size_t disknb = 0; disknb < 24; ++disknb )
    {
      workers.emplace_back( Copy10GB, hbanb, disknb, buffer, length );
    }
  }

  for( auto &w : workers ) w.join();


  delete[] buffer;

  std::cout << "The End." << std::endl;

  return 0;
}
