/*
 * XrdFecOssFile.cc
 *
 *  Created on: Jun 15, 2016
 *      Author: simonm
 */

#include "XrdFecFile.hh"
#include "XrdFecSys.hh"
#include "XrdFecChunkReader.hh"
#include "XrdFecChunkWriter.hh"

#include "XrdPosix/XrdPosixMap.hh"

#include "XrdSfs/XrdSfsAio.hh"

#include "XrdFecError.hh"

#include <fcntl.h>

#include <array>
#include <algorithm>
#include <iterator>
#include <exception>

void DeallocArgs( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response, XrdCl::HostList *hostList )
{
  delete status;
  delete response;
  delete hostList;
}

class ReadHandler : public XrdCl::ResponseHandler
{
  public:

    ReadHandler( XrdSfsAio *aoip ) : mAoip( aoip ) {}

    //----------------------------------------------------------------------------
    // Handle the response
    //----------------------------------------------------------------------------
    virtual void HandleResponseWithHosts( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response, XrdCl::HostList *hostList )
    {
      if( status->IsOK() )
      {
        XrdCl::ChunkInfo *chunkInfo = 0;
        response->Get( chunkInfo );
        mAoip->Result = chunkInfo->length;
        delete chunkInfo;
      }
      else
        mAoip->Result = -status->errNo;
      mAoip->doneRead();
      DeallocArgs( status, response, hostList );
      delete this;
    }

  private:
    XrdSfsAio *mAoip;
};

class WriteHandler : public XrdCl::ResponseHandler
{
  public:

    WriteHandler( XrdSfsAio *aoip, size_t blen ) : mAoip( aoip ), mBlen( blen ) {}

    //----------------------------------------------------------------------------
    // Handle the response
    //----------------------------------------------------------------------------
    virtual void HandleResponseWithHosts( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response, XrdCl::HostList *hostList )
    {
      mAoip->Result = status->IsOK() ? mBlen : -status->errNo;
      mAoip->doneWrite();
      DeallocArgs( status, response, hostList );
      delete this;
    }

  private:
    XrdSfsAio *mAoip;
    size_t mBlen;
};

const size_t XrdFecFile::k4KB = 4 * 1024;

XrdFecFile::XrdFecFile( XrdFecSys &sys ) : mSys( sys ), mRedundancy( sys.kNbData, sys.kNbParity ), kChunkSize( mRedundancy.numData() * k4KB )
{
}

void XrdFecFile::CleanFiles()
{
  for( auto fptr : mFiles ) delete fptr;
  mFiles.clear();
}

XrdFecFile::~XrdFecFile()
{
  CleanFiles();
  if( fd != -1 ) mSys.ReleaseFD( fd );
}

int XrdFecFile::Open( const char *path, int flags, mode_t mode, XrdOucEnv &env )
{
  CleanFiles();
  mFiles.reserve( mRedundancy.size() );
  for( size_t i = 0; i < mRedundancy.size(); ++i )
  {
    std::string url = "root://" + mSys.mHostIDs[i] + mSys.mPrefixes[i] + path;
    XrdCl::File *fptr = new XrdCl::File();
    XrdCl::XRootDStatus st = fptr->Open( url, XrdFecSys::ToOpenFlags( flags ), XrdPosixMap::Mode2Access( mode ) );
    if( !st.IsOK() ) return -st.errNo;
    mFiles.push_back( fptr );
  }
  fd = mSys.GetFD( this );
  return XrdOssOK;
}

int XrdFecFile::Close( long long *retsz )
{
  mSys.ReleaseFD( fd );
  fd = -1;
  int rc = XrdOssOK;
  for( auto fptr : mFiles )
  {
    XrdCl::XRootDStatus st = fptr->Close();
    if( !st.IsOK( ))
    {
      rc = -st.errNo;
      break;
    }
  }
  CleanFiles();
  return rc;
}

ssize_t XrdFecFile::Read( off_t offset, size_t blen )
{
  return XrdOssOK;
}

ssize_t XrdFecFile::Read( void *buff, off_t offset, size_t blen )
{
  char *buffer     = reinterpret_cast<char*>( buff );
  off_t begin;
  size_t first     = FirstChunk( offset, begin );
  size_t last      = LastChunk( offset, blen );
  size_t bytesRead = 0;

  XrdFecChunkReader reader( mSys, mFiles, first, last );
  std::string chunk;

  while( reader.Get( chunk ) )
  {
//    if( chunk.empty() ) TODO
    size_t size = chunk.size() - begin;
    if( blen < size ) size = blen;
    memcpy( buffer + bytesRead, chunk.c_str() + begin, size );
    bytesRead += size;
    blen      -= size;
    begin      = 0; // only in the first round begin matters
  }

  return bytesRead;
}

int XrdFecFile::Read( XrdSfsAio *aiop )
{
  return -ENOTSUP; // TODO
//  off_t offset = aiop->sfsAio.aio_offset;
//  size_t blen  = aiop->sfsAio.aio_nbytes;
//  void *buff   = const_cast<void*>( aiop->sfsAio.aio_buf );
//  ReadHandler *handler = new ReadHandler( aiop );
//  XrdCl::XRootDStatus st = mFiles[0]->Read( offset, blen, buff, handler );
//  if( !st.IsOK() )
//  {
//    delete handler;
//    return -st.errNo;
//  }
//  return XrdOssOK;
}

ssize_t XrdFecFile::ReadRaw( void *buff, off_t offset, size_t blen )
{
  return Read( buff, offset, blen );
}

int XrdFecFile::Fstat( struct stat *buff )
{
  return -ENOTSUP; // TODO
//  XrdCl::StatInfo *info = 0;
//  XrdCl::XRootDStatus st = mFiles[0]->Stat( true, info );
//  if( !st.IsOK() )
//  {
//    delete info;
//    return -st.errNo;
//  }
//  if( !info ) return -EBADF;
//  mSys.InfoToStat( info, buff );
//  delete info;
//  return XrdOssOK;
}

ssize_t XrdFecFile::Write( const void *buff, off_t offset, size_t blen )
{
  const char *buffer = reinterpret_cast<const char*>( buff );
  off_t begin;
  size_t first = FirstChunk( offset, begin );
  size_t last  = LastChunk( offset, blen );
  size_t bytesWritten = 0;
  try
  {
    XrdFecChunkReader reader( mSys, mFiles, first, last );
    XrdFecChunkWriter writer( mSys, mFiles, first, last );
    std::string chunk;

    while( reader.Get( chunk ) )
    {
  //    if( chunk.empty() ) TODO
      size_t size = chunk.size() - begin;
      if( blen < size ) size = blen;
      const char *b = buffer + bytesWritten, *e = b + size;
      std::string::iterator it = chunk.begin() + begin;
      std::copy( b, e, it );
      bytesWritten += size;
      blen         -= size;
      begin         = 0; // only in the first round begin matters
      PadChunk( chunk );
      writer.Put( chunk );
    }
    writer.Flush();
  }
  catch( XrdFecError &err )
  {
    return -err.Errno();
  }

  XrdCl::XRootDStatus st = mFiles[0]->Write( offset, blen, buff );
  if( !st.IsOK() ) return -st.errNo;
  return blen;
}

int XrdFecFile::Write( XrdSfsAio *aiop )
{
  return -ENOTSUP; // TODO
//  off_t offset = aiop->sfsAio.aio_offset;
//  size_t blen  = aiop->sfsAio.aio_nbytes;
//  void *buff   = const_cast<void*>( aiop->sfsAio.aio_buf );
//  WriteHandler *handler = new WriteHandler( aiop, blen );
//  XrdCl::XRootDStatus st = mFiles[0]->Write( offset, blen, buff, handler );
//  if( !st.IsOK() )
//  {
//    delete handler;
//    return -st.errNo;
//  }
//  return 0;
}

int XrdFecFile::Fsync()
{
  return -ENOTSUP; // TODO
//  XrdCl::XRootDStatus st = mFiles[0]->Sync();
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecFile::Ftruncate( unsigned long long size )
{
  return -ENOTSUP; // TODO
//  XrdCl::XRootDStatus st = mFiles[0]->Truncate( size );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

size_t XrdFecFile::FirstChunk( off_t offset, off_t &begin )
{
  begin = offset % kChunkSize;
  return  offset / kChunkSize;
}

size_t XrdFecFile::LastChunk( off_t offset, size_t blen )
{
  return ( offset + blen - 1 ) / kChunkSize;
}

void XrdFecFile::PadChunk( std::string & chunk )
{
  chunk.resize( kChunkSize, '\0' );
}
