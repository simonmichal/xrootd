/*
 * BlockAllocator.hh
 *
 *  Created on: Sep 20, 2016
 *      Author: simonm
 */

#ifndef SRC_API_TEST_BLOCKALLOCATOR_HH_
#define SRC_API_TEST_BLOCKALLOCATOR_HH_

#include <climits>
#include <list>
#include <map>
#include <string>

namespace XrdCl
{

class BlockAllocator
{
  public:

    struct Block
    {
        Block( uint64_t offset = 0, uint64_t size = 0 ) : offset( offset ), size( size ) { }

        uint64_t offset;
        uint64_t size;

        static bool Compare( const Block &b1, const Block &b2 )
        {
          return b1.offset < b2.offset;
        }
    };

    typedef std::list<Block> Blocks;

    struct Allocation
    {
        Allocation( const std::string &name, uint64_t size = 0, uint64_t currOffset = 0 ) : name( name ), size( size ), currOffset( currOffset ) { }

        std::string name;
        uint64_t    size;
        uint64_t    currOffset;
    };

    typedef std::list<Allocation> Allocations;

    typedef std::map<std::string, Allocation> Results;

    BlockAllocator() { }

    virtual ~BlockAllocator() { }

    void Realloc( Allocations &allocs, Blocks &blocks )
    {
      if( allocs.empty() ) return;

      // find max allocation
      Allocations::iterator maxA = allocs.begin();
      for( Allocations::iterator itr = allocs.begin(); itr != allocs.end(); ++itr )
      {
        if( itr->size > maxA->size ) maxA = itr;
      }

      // find the max slot
      Blocks::iterator maxB  = blocks.begin();
      Blocks::iterator nextB = blocks.begin();
      for( Blocks::iterator itr = blocks.begin(); itr != blocks.end(); ++itr )
      {
        if( itr->size > maxB->size ) maxB = itr;
        if( maxA->currOffset != nextB->offset ) ++nextB;
      }

      if( nextB != blocks.end() )
      {
        if( nextB->size >= maxA->size )
          maxB = nextB;
        else if( nextB ->size < maxA->size && maxB->size < maxA->size )
          maxB = nextB;
      }

      if( maxA->size == maxB->size )
      {
        result[maxA->name].push_back( Block( maxB->offset, maxB->size ) );
        allocs.erase( maxA );
        blocks.erase( maxB );
      }
      else if( maxA->size < maxB->size )
      {
        if( maxB->offset == maxA->currOffset )
        {
          result[maxA->name].push_back( Block( maxB->offset, maxA->size) );
          maxB->size   -= maxA->size;
          maxB->offset += maxA->size;
        }
        else
        {
          uint64_t offset = maxB->offset + maxB->size - maxA->size;
          result[maxA->name].push_back( Block( offset, maxA->size) );
          maxB->size -= maxA->size;
        }
        allocs.erase( maxA );
      }
      else
      {
        result[maxA->name].push_back( Block( maxB->offset, maxB->size ) );
        maxA->size       -= maxB->size;
        maxA->currOffset  = maxB->offset + maxB->size;
        blocks.erase( maxB );
      }

      Realloc( allocs, blocks );

      for( std::map<std::string, Blocks>::iterator itr = result.begin(); itr != result.end(); ++itr )
      {
        Blocks &blocks = itr->second;
        blocks.sort( Block::Compare );
      }
    }

    static int Cost( const Allocation &a, const Block &b )
    {
      if( a.currOffset != b.offset ) return 1;
      return 0;
    }

    void Print() const
    {
      typedef std::map< std::string, Blocks > Results;

      for( Results::const_iterator itrR = result.begin(); itrR != result.end(); ++itrR )
      {
        const Blocks &blocks = itrR->second;
        std::cout << itrR->first << '\n';
        for( Blocks::const_iterator itrB = blocks.begin(); itrB != blocks.end(); ++itrB )
        {
          std::cout << std::string( 5, ' ' ) << itrB->offset << ", " << itrB->size << '\n';
        }
      }
    }

  private:

    std::map< std::string, Blocks > result;
};

} /* namespace XrdCl */

#endif /* SRC_API_TEST_BLOCKALLOCATOR_HH_ */
