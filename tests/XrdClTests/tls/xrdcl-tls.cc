#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClMessageUtils.hh"
#include "XrdCl/XrdClDefaultEnv.hh"

#include <iostream>

template<typename FUNC, typename ... ARGs>
inline auto test( FUNC func, ARGs... args )
{
  using RET = typename std::result_of<FUNC(ARGs...)>::type;
  RET r{};
  return r;
}

int function( int, int )
{
  return 0;
}

std::string function2( int, int )
{
  return "";
}

int main( int argc, char *argv[] )
{
  using namespace XrdCl;

  std::cout << "There we go ..." << std::endl;

  std::cout << test( function2, 1, 1 ) << std::endl;

  std::cout << "The End." << std::endl;

  return 0;

  return 0;
}
