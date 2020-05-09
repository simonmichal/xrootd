/*
 * epn_load_gen.cc
 *
 *  Created on: 8 May 2020
 *      Author: simonm
 */


#include "StreamSimulator.hh"

#include <thread>
#include <iostream>

void run_stream( PlacementHandler *plhandler, uint8_t srvnb, uint8_t strmnb )
{
  StreamSimulator simulator( *plhandler, srvnb, strmnb );
  simulator.Start();
}

int main( int argc, char **argv )
{
  if( argc != 3 )
  {
    std::cout << "Expecting two arguments!" << std::endl;
    return 1;
  }

  std::string srvnb_str   = argv[1];
  std::string strmcnt_str = argv[2];

  uint8_t srvnb   = std::stoi( srvnb_str );   //< 0..3
  uint8_t strmcnt = std::stoi( strmcnt_str ); //< 1..50

  PlacementHandler plhandler( srvnb );
  std::vector<std::thread> threads;
  threads.reserve( strmcnt );

  for( uint8_t strmnb = 0; strmnb < strmcnt; ++strmnb )
    threads.emplace_back( run_stream, &plhandler, srvnb, strmnb );

  for( uint8_t strmnb = 0; strmnb < strmcnt; ++strmnb )
    threads[strmnb].join();

  return 0;
}
