//============================================================================
// Name        : xrd_api_test.cpp
// Author      : Michal Simon
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClMessageUtils.hh"
#include "XrdCl/XrdClFileStateHandler.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClConstants.hh"
#include "XrdCl/XrdClFileOperations.hh"

#include "XrdEc/XrdEcConfig.hh"
#include "XrdEc/XrdEcUtilities.hh"
//#include "XrdEc/XrdEcMetadataProvider.hh"
#include "XrdEc/XrdEcUtilities.hh"

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <set>

#include <random>
#include <functional>
#include <limits>
#include <random>
#include <thread>
#include <future>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <malloc.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <uuid/uuid.h>
#include <stdlib.h>
#include <random>

#include "../XrdEc/XrdEcDataObject.hh"

//#include "XrdEc/XrdEcUtilities.hh"
//#include "XrdEc/XrdEcDataStore.hh"


#include "XrdOuc/XrdOucEnv.hh"

using namespace XrdCl;
using namespace XrdEc;

bool WriteBible( DataObject &store, char *bigbuff, size_t biblesize )
{
  char    *wrtbuff = bigbuff;
  size_t   wrtsize = biblesize;
  uint64_t wrtoff = 0;
  while( wrtsize > 0 )
  {
    uint32_t chsize = 1024 * 3;
    if( chsize > wrtsize ) chsize = wrtsize;
    SyncResponseHandler handler;
    store.StrmWrite( wrtoff, chsize, wrtbuff, &handler );

    handler.WaitForResponse();
    XRootDStatus *status = handler.GetStatus();
    if( !status->IsOK() )
    {
      std::cout << status->ToString() << std::endl;
      return false;
    }
    delete status;

    wrtsize -= chsize;
    wrtoff  += chsize;
    wrtbuff += chsize;
  }
  store.Flush();

  SyncResponseHandler handler;
  store.Sync( &handler );
  handler.WaitForResponse();
  XRootDStatus *status = handler.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << "Sync failed!" << std::endl;
    std::cout << status->ToString() << std::endl;
    std::cout << status->GetErrorMessage() << std::endl;
    return false;
  }
  else
    std::cout << "Wrote whole bible into data store!" << std::endl;
  delete status;

  return true;
}

bool ReadTest( DataObject &store, char *bigbuff, size_t biblesize, uint32_t chsize )
{
  size_t rdsize = 0;
  int chnb = 0;
  uint64_t offset = 0;
  char *biblechunk = bigbuff;

  while( rdsize < biblesize )
  {
    char rdbuff[chsize];
    SyncResponseHandler handler2;
    store.StrmRead( offset, chsize, rdbuff, &handler2 );
    handler2.WaitForResponse();
    XRootDStatus *status = handler2.GetStatus();
    if( !status->IsOK() )
    {
      std::cout << status->ToString() << std::endl;
      return false;
    }
    delete status;

    AnyObject *resp = handler2.GetResponse();
    ChunkInfo *info = 0;
    resp->Get( info );
    uint32_t length = info->length;
    delete resp;

    bool buffeq = std::equal( rdbuff, rdbuff + length, biblechunk );
//    std::cout << "Chunk " << chnb << " : " << ( buffeq ? "EQUAL" : "NOT EQUAL" ) << std::endl;
//    std::cout << "info->length = " << info->length << std::endl;
    if( !buffeq )
    {
      std::cout << "\nReference:" << std::endl;
      std::cout << std::string( biblechunk, length ) << std::endl;
      std::cout << "\nData read:" << std::endl;
      std::cout << std::string( rdbuff, length ) << std::endl;
      return false;
    }

    rdsize     += length;
    offset     += length;
    biblechunk += length;

    ++chnb;
  }

  return true;
}

bool ReadTestChunk1024( DataObject &store, char *bigbuff, size_t biblesize )
{
  return ReadTest( store, bigbuff, biblesize, 1024 );
}

bool ReadTestChunk3072( DataObject &store, char *bigbuff, size_t biblesize )
{
  return ReadTest( store, bigbuff, biblesize, 3072 );
}

bool ReadTestChunk4096( DataObject &store, char *bigbuff, size_t biblesize )
{
  return ReadTest( store, bigbuff, biblesize, 4096 );
}

bool ReadTestChunk8192( DataObject &store, char *bigbuff, size_t biblesize )
{
  return ReadTest( store, bigbuff, biblesize, 8192 );
}

bool ReadTestChunk10240( DataObject &store, char *bigbuff, size_t biblesize )
{
  return ReadTest( store, bigbuff, biblesize, 10240 );
}

bool ReadTestChunk16384( DataObject &store, char *bigbuff, size_t biblesize )
{
  return ReadTest( store, bigbuff, biblesize, 16384 );
}

bool ReadTestAll( DataObject &store, char *bigbuff, size_t biblesize )
{
  return ReadTest( store, bigbuff, biblesize, biblesize );
}

bool RandReadTest( DataObject &store, char *bigbuff, size_t biblesize, uint64_t offset, uint32_t length )
{
  char     buffer[length];

  XrdCl::SyncResponseHandler handler;
  store.RandomRead( offset, length, buffer, &handler );
  handler.WaitForResponse();
  XRootDStatus *status = handler.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  AnyObject *resp = handler.GetResponse();
  ChunkInfo *info = 0;
  resp->Get( info );

  uint32_t explen  = length;
  if( offset + explen > biblesize ) explen = biblesize - offset;
  uint32_t bytesrd = info->length;
  delete resp;

  if( bytesrd != explen )
  {
    std::cout << "Bytes read mismatch : expected = " << explen << ", got = " << bytesrd << std::endl;
    return false;
  }

  auto begin = bigbuff + offset;
  auto end   = begin + bytesrd;

  bool buffeq = std::equal( begin, end, buffer );
  if( !buffeq )
  {
    std::cout << "begin[0] = " << int( buffer[0] ) << std::endl;

    std::cout << "\n\nReference:" << std::endl;
    std::cout << std::string( begin, 4 * 1024 ) << std::endl;
    std::cout << "\n\nData read:" << std::endl;
    std::cout << std::string( buffer, 4 * 1024 ) << std::endl;
    std::cout << "Bytes read don't match the reference!" << std::endl;
    return false;
  }

  return true;
}

bool RandReadTestChunk_o1024_l1024( DataObject &store, char *bigbuff, size_t biblesize )
{
  // both offset and size are aligned to our data chunks
  return RandReadTest( store, bigbuff, biblesize, 1024, 1024 );
}

bool RandReadTestChunk_o1280_l1024( DataObject &store, char *bigbuff, size_t biblesize )
{
  // both offset and size are NOT aligned to our data chunks
  return RandReadTest( store, bigbuff, biblesize, 1280, 1024 );
}

bool RandReadTestChunk_o3840_l512( DataObject &store, char *bigbuff, size_t biblesize )
{
  // read across the border of two blocks
  return RandReadTest( store, bigbuff, biblesize, 3840, 512 );
}

bool RandReadTestChunk_o3840_l1024( DataObject &store, char *bigbuff, size_t biblesize )
{
  // read across the border of two blocks and 3 chunks
  return RandReadTest( store, bigbuff, biblesize, 3840, 1024 );
}

bool RandReadTestChunk_o1280_l8192( DataObject &store, char *bigbuff, size_t biblesize )
{
  // both offset and size are NOT aligned to our data chunks
  return RandReadTest( store, bigbuff, biblesize, 1280, 8192 );
}

bool RandTestRandomReadBigChunk( DataObject &store, char *bigbuff, size_t biblesize )
{
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator( seed );
  std::uniform_int_distribution<size_t> offdistr( 0, biblesize );
  uint64_t offset = offdistr( generator );
  std::uniform_int_distribution<size_t> lendistr( offset, biblesize + 1024 * 6 );
  uint32_t length = lendistr( generator );
  std::cout << "RandTestRandomReadBigChunk : seed = " << seed << std::endl;
  return RandReadTest( store, bigbuff, biblesize, offset, length );
}

bool RandTestRandomReadSmallChunk( DataObject &store, char *bigbuff, size_t biblesize )
{
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator( seed );
  std::uniform_int_distribution<size_t> offdistr( 0, biblesize - 4 * 4 * 1024 );
  uint64_t offset = offdistr( generator );
  std::uniform_int_distribution<size_t> lendistr( 256, 4 * 4 * 1024 );
  uint32_t length = lendistr( generator );
  std::cout << "RandTestRandomReadSmallChunk : seed = " << seed << std::endl;
  return RandReadTest( store, bigbuff, biblesize, offset, length );
}

bool ReadTestRandomChunkBig( DataObject &store, char *bigbuff, size_t biblesize )
{
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator( seed );
  std::uniform_int_distribution<size_t> distribution( 512, biblesize );
  uint32_t chsize = distribution( generator );
  std::cout << "ReadTestRandomChunkBig : seed = " << seed << std::endl;
  return ReadTest( store, bigbuff, biblesize, chsize );
}

bool ReadTestRandomChunkSmall( DataObject &store, char *bigbuff, size_t biblesize )
{
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator( seed );
  std::uniform_int_distribution<size_t> distribution( 256, 4 * 4 * 1024  );
  uint32_t chsize = distribution( generator );
  std::cout << "ReadTestRandomChunkSmall : seed = " << seed << std::endl;
  return ReadTest( store, bigbuff, biblesize, chsize );
}

bool TruncateStore( DataObject &store, uint64_t size )
{
  SyncResponseHandler handler0;
  store.Truncate( size, &handler0 );
  handler0.WaitForResponse();
  XRootDStatus *status = handler0.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  return true;
}

bool SparseFileTest( char *bigbuff, size_t biblesize )
{
  //---------------------------------------------------------------------
  // We are testing following scenario:
  //
  // Block:    0    1    2    3    4    5    6    7
  //         ---- ---- ---- ---- ---- ---- ---- ----
  //        |    |    |    |    |    |    |    |    |
  //         ----      ----        -- -  - --   ----
  //
  // Empty space represents sparseness in the object!
  //
  //---------------------------------------------------------------------

  Config &cfg = Config::Instance();

  DataObject object( "SparseFileTest.txt", biblesize );

  //---------------------------------------------------------------------
  // Make sure the file size is 0
  //---------------------------------------------------------------------

  if( !TruncateStore( object, 0 ) )
  {
    std::cout << "Failed to truncate the object!" << std::endl;
    return false;
  }

  //---------------------------------------------------------------------
  // prepare for writing
  //---------------------------------------------------------------------

  uint64_t refsize = cfg.datasize * 8;
  char reference[refsize];
  memset( reference, 0, refsize );

  uint64_t offset    = 0;
  uint64_t refcursor = 0;

  //---------------------------------------------------------------------
  // write the first block at offset 0
  //---------------------------------------------------------------------

  // write first data block from the bible
  SyncResponseHandler handler;
  object.StrmWrite( offset, cfg.datasize, bigbuff, &handler );
  handler.WaitForResponse();
  XRootDStatus *status = handler.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  memcpy( reference + refcursor, bigbuff, cfg.datasize );

  offset    += cfg.datasize;
  bigbuff   += cfg.datasize;
  refcursor += cfg.datasize;

  //---------------------------------------------------------------------
  // write the second block at offset 2 * datasize
  //---------------------------------------------------------------------

  offset += cfg.datasize;
  SyncResponseHandler handler2;
  object.StrmWrite( offset, cfg.datasize, bigbuff, &handler2 );
  handler2.WaitForResponse();
  status = handler2.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += cfg.datasize;
  memcpy( reference + refcursor, bigbuff, cfg.datasize );

  offset    += cfg.datasize;
  bigbuff   += cfg.datasize;
  refcursor += cfg.datasize;

  //---------------------------------------------------------------------
  // write 1/2 of the third block at offset 4.5 * datasize
  //---------------------------------------------------------------------

  offset += 1.5 * cfg.datasize;
  SyncResponseHandler handler3;
  object.StrmWrite( offset, cfg.datasize * 0.5, bigbuff, &handler3 );
  handler3.WaitForResponse();
  status = handler3.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += 1.5 * cfg.datasize;
  memcpy( reference + refcursor, bigbuff, 0.5 * cfg.datasize );

  offset    += 0.5 * cfg.datasize;
  bigbuff   += int( 0.5 * cfg.datasize );
  refcursor += 0.5 * cfg.datasize;

  //---------------------------------------------------------------------
  // write 1/4 of the fourth block at offset 5 * datasize
  //---------------------------------------------------------------------

  SyncResponseHandler handler4;
  object.StrmWrite( offset, cfg.datasize * 0.25, bigbuff, &handler4 );
  handler4.WaitForResponse();
  status = handler4.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  memcpy( reference + refcursor, bigbuff, 0.25 * cfg.datasize );

  offset    += 0.25 * cfg.datasize;
  bigbuff   += int( 0.25 * cfg.datasize );
  refcursor += 0.25 * cfg.datasize;

  //---------------------------------------------------------------------
  // write subsequent 1/4 of the fourth block at offset 5.75 * datasize
  //---------------------------------------------------------------------

  offset += 0.5 * cfg.datasize;
  SyncResponseHandler handler5;
  object.StrmWrite( offset, cfg.datasize * 0.25, bigbuff, &handler5 );
  handler5.WaitForResponse();
  status = handler5.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += 0.5 * cfg.datasize;
  memcpy( reference + refcursor, bigbuff, 0.25 * cfg.datasize );

  offset    += 0.25 * cfg.datasize;
  bigbuff   += int( 0.25 * cfg.datasize );
  refcursor += 0.25 * cfg.datasize;

  //---------------------------------------------------------------------
  // write 1/2 of the fifth block at offset 6 * datasize
  //---------------------------------------------------------------------

  SyncResponseHandler handler6;
  object.StrmWrite( offset, cfg.datasize * 0.5, bigbuff, &handler6 );
  handler6.WaitForResponse();
  status = handler6.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  memcpy( reference + refcursor, bigbuff, 0.5 * cfg.datasize );

  offset    += 0.5 * cfg.datasize;
  bigbuff   += int( 0.5 * cfg.datasize );
  refcursor += 0.5 * cfg.datasize;

  //---------------------------------------------------------------------
  // write one block of data at offset 7 * datasize
  //---------------------------------------------------------------------

  offset += 0.5 * cfg.datasize;
  SyncResponseHandler handler7;
  object.StrmWrite( offset, cfg.datasize, bigbuff, &handler7 );
  handler7.WaitForResponse();
  status = handler7.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += 0.5 * cfg.datasize;
  memcpy( reference + refcursor, bigbuff, cfg.datasize );

  offset    += cfg.datasize;
  bigbuff   += cfg.datasize;
  refcursor += cfg.datasize;

  //---------------------------------------------------------------------
  // Sync to be sure everything was written correctly to disk
  //---------------------------------------------------------------------
  object.Flush();
  SyncResponseHandler handler8;
  object.Sync( &handler8 );
  handler8.WaitForResponse();
  status = handler8.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << "Sync failed!" << std::endl;
    std::cout << status->ToString() << std::endl;
    std::cout << status->GetErrorMessage() << std::endl;
    return false;
  }
  else
    std::cout << "Wrote whole sparse-write-test data!" << std::endl;
  delete status;

  //---------------------------------------------------------------------
  // now read all the staff we have written and compare with reference
  //---------------------------------------------------------------------

  char rdbuff[refsize];
  SyncResponseHandler handler9;
  object.StrmRead( 0, refsize, rdbuff, &handler9 );
  handler9.WaitForResponse();
  status = handler9.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  AnyObject *resp = handler9.GetResponse();
  ChunkInfo *info = 0;
  resp->Get( info );
  if( info->length != refsize )
  {
    std::cout << "Number of bytes read is incorrect!" << std::endl;
    std::cout << "info->length = " << info->length << ", refsize = " << refsize << std::endl;
    return false;
  }
  delete resp;

  if( !std::equal( rdbuff, rdbuff + refsize, reference ) )
  {
    std::cout << "Reference buffer does not much data read from data store!" << std::endl;
    return false;
  }

  return true;
}



int main( int argc, char** argv )
{
  std::cout << "There we go!" << std::endl;

  std::string path = "bible.txt";

  size_t biblesize = 4047392;
  char *bigbuff = new char[biblesize + 1024]; // big buffer than can accommodate the whole bible plus bit more
  std::ifstream fs;
  fs.open( path, std::fstream::in );
  fs.read( bigbuff, biblesize );
  fs.close();
  bigbuff[biblesize] = 0;

  std::cout << "biblesize = " << biblesize << std::endl;
//  std::cout << bigbuff << std::endl;

  if( !SparseFileTest( bigbuff, biblesize ) )
  {
    std::cout << "SparseFileTest: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "SparseFileTest: succeeded!" << std::endl;

  DataObject object( path, biblesize );

  if( !TruncateStore( object, 0 ) )
  {
    std::cout << "TruncateStore: failed!" << std::endl;
    return 1;
  }

  if( !WriteBible( object, bigbuff, biblesize ) )
  {
    std::cout << "WriteBible: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "WriteBible: succeeded!" << std::endl;

  if( !ReadTestChunk1024( object, bigbuff, biblesize ) )
  {
    std::cout << "ReadTestChunk1024: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "ReadTestChunk1024: succeeded!" << std::endl;

  if( !ReadTestChunk3072( object, bigbuff, biblesize ) )
  {
    std::cout << "ReadTestChunk3072: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "ReadTestChunk3072: succeeded!" << std::endl;

  if( !ReadTestChunk4096( object, bigbuff, biblesize ) )
  {
    std::cout << "ReadTestChunk4096: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "ReadTestChunk4096: succeeded!" << std::endl;

  if( !ReadTestChunk8192( object, bigbuff, biblesize ) )
  {
    std::cout << "ReadTestChunk8192: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "ReadTestChunk8192: succeeded!" << std::endl;

  if( !ReadTestChunk10240( object, bigbuff, biblesize ) )
  {
    std::cout << "ReadTestChunk10240: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "ReadTestChunk10240: succeeded!" << std::endl;

  if( !ReadTestChunk16384( object, bigbuff, biblesize ) )
  {
    std::cout << "ReadTestChunk16384: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "ReadTestChunk16384: succeeded!" << std::endl;

  if( !ReadTestAll( object, bigbuff, biblesize ) )
  {
    std::cout << "ReadTestAll: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "ReadTestAll: succeeded!" << std::endl;

  for( size_t i = 0; i < 100; ++i )
  {
    if( !ReadTestRandomChunkBig( object, bigbuff, biblesize ) )
    {
      std::cout << i << ": ReadTestRandomChunkBig: failed!" << std::endl;
      return 1;
    }
    else
      std::cout << i << ": ReadTestRandomChunkBig: succeeded!" << std::endl;
  }

  for( size_t i = 0; i < 100; ++i )
  {
    if( !ReadTestRandomChunkSmall( object, bigbuff, biblesize ) )
    {
      std::cout << i << ": ReadTestRandomChunkSmall: failed!" << std::endl;
      return 1;
    }
    else
      std::cout << i << ": ReadTestRandomChunkSmall: succeeded!" << std::endl;
  }

  if( !RandReadTestChunk_o1024_l1024( object, bigbuff, biblesize ) )
  {
    std::cout << "RandReadTestChunk_o1024_l1024: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "RandReadTestChunk_o1024_l1024: succeeded!" << std::endl;

  if( !RandReadTestChunk_o1280_l1024( object, bigbuff, biblesize ) )
  {
    std::cout << "RandReadTestChunk_o1280_l1024: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "RandReadTestChunk_o1280_l1024: succeeded!" << std::endl;

  if( !RandReadTestChunk_o3840_l512( object, bigbuff, biblesize ) )
  {
    std::cout << "RandReadTestChunk_o3840_l512: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "RandReadTestChunk_o3840_l512: succeeded!" << std::endl;

  if( !RandReadTestChunk_o3840_l1024( object, bigbuff, biblesize ) )
  {
    std::cout << "RandReadTestChunk_o3840_l1024: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "RandReadTestChunk_o3840_l1024: succeeded!" << std::endl;

  if( !RandReadTestChunk_o1280_l8192( object, bigbuff, biblesize ) )
  {
    std::cout << "RandReadTestChunk_o1280_l8192: failed!" << std::endl;
    return 1;
  }
  else
    std::cout << "RandReadTestChunk_o1280_l8192: succeeded!" << std::endl;

  for( size_t i = 0; i < 100; ++i )
  {
    if( !RandTestRandomReadBigChunk( object, bigbuff, biblesize ) )
    {
      std::cout << i << ": RandTestRandomReadBigChunk: failed!" << std::endl;
      return 1;
    }
    std::cout << i << ": RandTestRandomReadBigChunk: succeeded!" << std::endl;
  }

  for( size_t i = 0; i < 100; ++i )
  {
    if( !RandTestRandomReadSmallChunk( object, bigbuff, biblesize ) )
    {
      std::cout << i << ": RandTestRandomReadSmallChunk: failed!" << std::endl;
      return 1;
    }
    std::cout << i << ": RandTestRandomReadSmallChunk: succeeded!" << std::endl;
  }

  delete[] bigbuff;
  std::cout << "The End." << std::endl;

  return 0;
}


