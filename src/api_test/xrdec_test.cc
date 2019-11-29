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
#include "XrdEc/XrdEcLogger.hh"
//#include "XrdEc/XrdEcMetadataProvider.hh"
#include "XrdEc/XrdEcUtilities.hh"

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <set>

#include <random>
#include <functional>
#include <limits>
#include <thread>
#include <future>
#include <cstdio>

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
#include <sys/stat.h>

#include "../XrdEc/XrdEcDataObject.hh"

#include "XrdEc/XrdEcObjCfg.hh"


#include "XrdOuc/XrdOucEnv.hh"

using namespace XrdCl;
using namespace XrdEc;

const std::string objname( "0000000000000001-isa-12:4-4.0" );

static ObjCfg objcfg( objname ); // the configuration we use !!!

inline placement_group& GetPlacement()
{
  static placement_group plgr;
  if( plgr.empty() )
  {
    for( size_t i = 0; i < 16; ++i )
    {
      std::string url = "file://localhost/data/dir" + std::to_string( i );
      plgr.emplace_back( url, rw );
    }
  }
  return plgr;
}

inline std::default_random_engine& GetGenerator()
{
  static unsigned seed = 2688813254;//std::chrono::system_clock::now().time_since_epoch().count();
  static std::default_random_engine generator( seed );
  static bool print = false;
  if( !print )
  {
    std::cout << __func__ << " : seed = " << seed << std::endl;
    print = true;
  }
  return generator;
}

inline std::string SelectChunk( uint64_t offset, uint32_t size )
{
  uint64_t blkfst = offset / objcfg.datasize;
  uint64_t blklst = ( offset + size ) / objcfg.datasize;
  if( ( offset + size ) % objcfg.datasize )
    blklst -= 1;

  uint64_t blknb;
  if( blklst <= blkfst ) blknb = blkfst;
  else
  {
    std::uniform_int_distribution<size_t> blkdistr( blkfst, blklst );
    blknb = blkdistr( GetGenerator() );
  }

  std::uniform_int_distribution<size_t> chdistr( 0, objcfg.nbchunks - 1 );
  uint8_t chnb = chdistr( GetGenerator() );
  std::string blkname = objname + '.' + std::to_string( blknb );
  placement_t placement = GeneratePlacement( objcfg, blkname, GetPlacement(), true );
  XrdCl::URL url( placement[chnb] + '/' + blkname );

  return url.GetURL();
}

inline bool RemoveChunk( const XrdCl::URL &url )
{
  std::cout << __func__ << " : url = " << url.GetPath() << std::endl;
  int rc = remove( url.GetPath().c_str() );
  if( rc != 0 && errno != ENOENT && errno != EACCES )
  {
    std::cout << strerror( errno ) << std::endl;
    return false;
  }

  return true;
}

inline bool RemoveChunk( uint64_t offset, uint32_t size )
{
  return RemoveChunk( SelectChunk( offset, size ) );
}

inline bool RejectAccess( const XrdCl::URL &url )
{
  mode_t mode;
  memset( &mode, 0, sizeof( mode ) );
  int rc = chmod( url.GetPath().c_str(), mode );
  if( rc != 0 && errno != ENOENT && errno != EACCES )
  {
    std::cout << strerror( errno ) << std::endl;
    return false;
  }

  return true;
}

inline bool RejectAccess( uint64_t offset, uint32_t size )
{
  XrdCl::URL url = SelectChunk( offset, size );
  std::cout << __func__ << " : url = " << url.GetPath() << std::endl;
  if( !RemoveChunk( url ) ) return false;
  std::ofstream chunk( url.GetPath() );
  chunk.flush();

  return RejectAccess( url );
}

inline bool CorruptChunk( uint64_t offset, uint32_t size )
{
  std::string str = "some random string that does not appear in bible ;-)";
  XrdCl::URL url = SelectChunk( offset, size );

  std::cout << __func__ << " : url = " << url.GetPath() << std::endl;

  std::ofstream chunk( url.GetPath() );
  chunk.write( str.c_str(), str.size() );
  chunk.flush();
  return true;
}


inline bool DoError( size_t upper )
{
  static std::uniform_int_distribution<size_t> prob( 1, upper );
  size_t randval = prob( GetGenerator() );
  return ( randval > upper - 1 );
}

bool RandomError( uint64_t offset, uint32_t size )
{
  enum ErrType
  {
    ACCESS,
    REMOVE,
    CORRUPT
  };

  std::uniform_int_distribution<size_t> prob( ACCESS, CORRUPT );
  size_t randval = prob( GetGenerator() );

  switch( randval )
  {
    case ACCESS : return RejectAccess( offset, size );
    case REMOVE : return RemoveChunk( offset, size );
    case CORRUPT: return CorruptChunk( offset, size );
    default: return false;
  }
}

bool WriteBible( DataObject &store, char *bigbuff, size_t biblesize )
{
  // select unavailable destination
  std::uniform_int_distribution<size_t> dirdistr( 0, 15 );
  size_t dirnb = dirdistr( GetGenerator() );
  std::string url = "file://localhost/data/dir" + std::to_string( dirnb );
  if( !RejectAccess( url ) )
  {
    std::cout << "RejectAccess failed!" << std::endl;
    return false;
  }
  else
    std::cout << "RejectAccess : " << url << std::endl;


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

  // we are done, everything is good so we can purge the log file
  Logger::Instance().Purge();

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
    if( DoError( 1000 ) )
      if( !RandomError( offset, chsize ) )
      {
        std::cout << "RandomError failed!" << std::endl;
        return false;
      }

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
    uint32_t explen  = length;
    if( offset + explen > biblesize ) explen = biblesize - offset;
    uint32_t bytesrd = length;
    delete resp;

    if( bytesrd != explen )
    {
      std::cout << "Bytes read mismatch : expected = " << explen << ", got = " << bytesrd << std::endl;

      std::cout << "\n\nReference:" << std::endl;
      std::cout << std::string( biblechunk, explen ) << std::endl;
      std::cout << "\n\nData read:" << std::endl;
      std::cout << std::string( rdbuff, bytesrd ) << std::endl;

      return false;
    }

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

  // we are done, everything is good so we can purge the log file
  Logger::Instance().Purge();

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

  if( DoError( 1000 ) )
    if( !RandomError( offset, length ) )
    {
      std::cout << "RandomError failed!" << std::endl;
      return false;
    }

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

  auto begin = bigbuff + offset;
  auto end   = begin + bytesrd;

  if( bytesrd != explen )
  {
    std::cout << "Bytes read mismatch : expected = " << explen << ", got = " << bytesrd << std::endl;

    std::cout << "\n\noffset = " << offset << ", length = " << length << std::endl;

    std::cout << "\n\nReference:" << std::endl;
    std::cout << std::string( begin, explen ) << std::endl;
    std::cout << "\n\nData read:" << std::endl;
    std::cout << std::string( buffer, bytesrd ) << std::endl;

    return false;
  }

  bool buffeq = std::equal( begin, end, buffer );
  if( !buffeq )
  {
    std::cout << "\n\nReference:" << std::endl;
    std::cout << std::string( begin, bytesrd ) << std::endl;
    std::cout << "\n\nData read:" << std::endl;
    std::cout << std::string( buffer, bytesrd ) << std::endl;
    std::cout << "Bytes read don't match the reference!" << std::endl;
    return false;
  }

  // we are done, everything is good so we can purge the log file
  Logger::Instance().Purge();

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
  std::uniform_int_distribution<size_t> offdistr( 0, biblesize );
  uint64_t offset = offdistr( GetGenerator() );
  std::uniform_int_distribution<size_t> lendistr( offset, biblesize + 1024 * 6 );
  uint32_t length = lendistr( GetGenerator() );
  return RandReadTest( store, bigbuff, biblesize, offset, length );
}

bool RandTestRandomReadSmallChunk( DataObject &store, char *bigbuff, size_t biblesize )
{
  std::uniform_int_distribution<size_t> offdistr( 0, biblesize - 4 * 4 * 1024 );
  uint64_t offset = offdistr( GetGenerator() );
  std::uniform_int_distribution<size_t> lendistr( 256, 4 * 4 * 1024 );
  uint32_t length = lendistr( GetGenerator() );
  return RandReadTest( store, bigbuff, biblesize, offset, length );
}

bool ReadTestRandomChunkBig( DataObject &store, char *bigbuff, size_t biblesize )
{
  std::uniform_int_distribution<size_t> distribution( 512, biblesize );
  uint32_t chsize = distribution( GetGenerator() );
  return ReadTest( store, bigbuff, biblesize, chsize );
}

bool ReadTestRandomChunkSmall( DataObject &store, char *bigbuff, size_t biblesize )
{
  std::uniform_int_distribution<size_t> distribution( 256, 4 * 4 * 1024  );
  uint32_t chsize = distribution( GetGenerator() );
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

  // we are done, everything is good so we can purge the log file
  Logger::Instance().Purge();

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

  uint64_t refsize = objcfg.datasize * 8;
  char reference[refsize];
  memset( reference, 0, refsize );

  uint64_t offset    = 0;
  uint64_t refcursor = 0;

  //---------------------------------------------------------------------
  // write the first block at offset 0
  //---------------------------------------------------------------------

  // write first data block from the bible
  SyncResponseHandler handler;
  object.StrmWrite( offset, objcfg.datasize, bigbuff, &handler );
  handler.WaitForResponse();
  XRootDStatus *status = handler.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  memcpy( reference + refcursor, bigbuff, objcfg.datasize );

  offset    += objcfg.datasize;
  bigbuff   += objcfg.datasize;
  refcursor += objcfg.datasize;

  //---------------------------------------------------------------------
  // write the second block at offset 2 * datasize
  //---------------------------------------------------------------------

  offset += objcfg.datasize;
  SyncResponseHandler handler2;
  object.StrmWrite( offset, objcfg.datasize, bigbuff, &handler2 );
  handler2.WaitForResponse();
  status = handler2.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += objcfg.datasize;
  memcpy( reference + refcursor, bigbuff, objcfg.datasize );

  offset    += objcfg.datasize;
  bigbuff   += objcfg.datasize;
  refcursor += objcfg.datasize;

  //---------------------------------------------------------------------
  // write 1/2 of the third block at offset 4.5 * datasize
  //---------------------------------------------------------------------

  offset += 1.5 * objcfg.datasize;
  SyncResponseHandler handler3;
  object.StrmWrite( offset, objcfg.datasize * 0.5, bigbuff, &handler3 );
  handler3.WaitForResponse();
  status = handler3.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += 1.5 * objcfg.datasize;
  memcpy( reference + refcursor, bigbuff, 0.5 * objcfg.datasize );

  offset    += 0.5 * objcfg.datasize;
  bigbuff   += int( 0.5 * objcfg.datasize );
  refcursor += 0.5 * objcfg.datasize;

  //---------------------------------------------------------------------
  // write 1/4 of the fourth block at offset 5 * datasize
  //---------------------------------------------------------------------

  SyncResponseHandler handler4;
  object.StrmWrite( offset, objcfg.datasize * 0.25, bigbuff, &handler4 );
  handler4.WaitForResponse();
  status = handler4.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  memcpy( reference + refcursor, bigbuff, 0.25 * objcfg.datasize );

  offset    += 0.25 * objcfg.datasize;
  bigbuff   += int( 0.25 * objcfg.datasize );
  refcursor += 0.25 * objcfg.datasize;

  //---------------------------------------------------------------------
  // write subsequent 1/4 of the fourth block at offset 5.75 * datasize
  //---------------------------------------------------------------------

  offset += 0.5 * objcfg.datasize;
  SyncResponseHandler handler5;
  object.StrmWrite( offset, objcfg.datasize * 0.25, bigbuff, &handler5 );
  handler5.WaitForResponse();
  status = handler5.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += 0.5 * objcfg.datasize;
  memcpy( reference + refcursor, bigbuff, 0.25 * objcfg.datasize );

  offset    += 0.25 * objcfg.datasize;
  bigbuff   += int( 0.25 * objcfg.datasize );
  refcursor += 0.25 * objcfg.datasize;

  //---------------------------------------------------------------------
  // write 1/2 of the fifth block at offset 6 * datasize
  //---------------------------------------------------------------------

  SyncResponseHandler handler6;
  object.StrmWrite( offset, objcfg.datasize * 0.5, bigbuff, &handler6 );
  handler6.WaitForResponse();
  status = handler6.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  memcpy( reference + refcursor, bigbuff, 0.5 * objcfg.datasize );

  offset    += 0.5 * objcfg.datasize;
  bigbuff   += int( 0.5 * objcfg.datasize );
  refcursor += 0.5 * objcfg.datasize;

  //---------------------------------------------------------------------
  // write one block of data at offset 7 * datasize
  //---------------------------------------------------------------------

  offset += 0.5 * objcfg.datasize;
  SyncResponseHandler handler7;
  object.StrmWrite( offset, objcfg.datasize, bigbuff, &handler7 );
  handler7.WaitForResponse();
  status = handler7.GetStatus();
  if( !status->IsOK() )
  {
    std::cout << status->ToString() << std::endl;
    return false;
  }
  delete status;

  // now do the same with the reference buffer
  refcursor += 0.5 * objcfg.datasize;
  memcpy( reference + refcursor, bigbuff, objcfg.datasize );

  offset    += objcfg.datasize;
  bigbuff   += objcfg.datasize;
  refcursor += objcfg.datasize;

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

  // we are done, everything is good so we can purge the log file
  Logger::Instance().Purge();

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
  else
    std::cout << "TruncateStore: succeeded!" << std::endl;

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


