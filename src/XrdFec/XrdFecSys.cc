/*
 * XrdFecOss.cc
 *
 *  Created on: Jun 15, 2016
 *      Author: simonm
 */

#include "XrdFecSys.hh"

#include "XrdFecDir.hh"
#include "XrdFecFile.hh"

#include "XrdVersion.hh"

#include "XrdCl/XrdClFileSystem.hh"

#include "XrdPosix/XrdPosixMap.hh"

#include <limits.h>
#include <fcntl.h>

extern "C"
{
  XrdOss* XrdOssGetStorageSystem( XrdOss* native_oss, XrdSysLogger* lp, const char* config_fn, const char* parms )
  {
    return new XrdFecSys();
  }
}

XrdVERSIONINFO( XrdOssGetStorageSystem, XrdFecSys );


const int XrdFecSys::kMinFD = 1;
const int XrdFecSys::kMaxFD = INT_MAX;

XrdFecSys::XrdFecSys() : mNextFD( kMinFD ), kNbData( 2 ), kNbParity( 2 )
{
  mPrefixes = { "/data/xrd-erasure01/", "/data/xrd-erasure02/", "/data/xrd-erasure03/", "/data/xrd-erasure04/" };
  mHostIDs  = { "localhost:2001/", "localhost:2002/", "localhost:2003/", "localhost:2004/" };
  mFSs.reserve( mHostIDs.size() );
  for( auto hostID : mHostIDs )
    mFSs.push_back( new XrdCl::FileSystem( hostID ) );
}

XrdFecSys::~XrdFecSys()
{
  for( auto fsptr : mFSs )
    delete fsptr;
}

XrdOssDF* XrdFecSys::newDir( const char *tident )
{
  return new XrdFecDir( *this );
}

XrdOssDF* XrdFecSys::newFile( const char *tident )
{
  return new XrdFecFile( *this );
}

int XrdFecSys::Chmod( const char *path, mode_t mode, XrdOucEnv *eP )
{
  return -ENOTSUP;
//  XrdCl::Access::Mode xmode = ToAccessMode( mode );
//  XrdCl::XRootDStatus st = mFs.ChMod( kPrefix + path, xmode );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecSys::Create( const char *tident, const char *path, mode_t mode, XrdOucEnv &, int opts )
{
  return -ENOTSUP;
//  XrdCl::File f;
//  std::string url = "root://" + kHostID + kPrefix + path;
//  XrdCl::OpenFlags::Flags flags = XrdCl::OpenFlags::New;
//  XrdCl::XRootDStatus st = f.Open( url, flags, ToAccessMode( mode ) );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecSys::Init( XrdSysLogger *, const char* )
{
  return XrdOssOK;
}

int XrdFecSys::Mkdir( const char *path, mode_t mode, int mkpath, XrdOucEnv *eP )
{
  return -ENOTSUP;
//  XrdCl::MkDirFlags::Flags flags = mkpath ? XrdCl::MkDirFlags::MakePath : XrdCl::MkDirFlags::None;
//  XrdCl::XRootDStatus st = mFs.MkDir( kPrefix + path, flags, ToAccessMode( mode ) );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecSys::Remdir( const char *path, int Opts, XrdOucEnv *eP )
{
  return -ENOTSUP;
//  XrdCl::XRootDStatus st = mFs.RmDir( kPrefix + path );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecSys::Rename( const char *from, const char *to, XrdOucEnv *eP1, XrdOucEnv *eP2 )
{
  return -ENOTSUP;
//  XrdCl::XRootDStatus st = mFs.Mv( kPrefix + from, kPrefix + to );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecSys::Stat( const char *path, struct stat* buff, int opts, XrdOucEnv *eP )
{
  return -ENOTSUP;
//  XrdCl::StatInfo *info = 0;
//  XrdCl::XRootDStatus st = mFs.Stat( kPrefix + path, info );
//  if( !st.IsOK() )
//  {
//    delete info;
//    return -st.errNo;
//  }
//  if( !info ) return -EBADF;
//  InfoToStat( info, buff );
//  delete info;
//  return XrdOssOK;
}

int XrdFecSys::Truncate( const char *path, unsigned long long size, XrdOucEnv *eP )
{
  return -ENOTSUP;
//  XrdCl::XRootDStatus st = mFs.Truncate( kPrefix + path, size );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecSys::Unlink( const char *path, int Opts, XrdOucEnv *eP )
{
  return -ENOTSUP;
//  XrdCl::XRootDStatus st = mFs.Rm( kPrefix + path );
//  if( !st.IsOK() ) return -st.errNo;
//  return XrdOssOK;
}

int XrdFecSys::GetFD( XrdOssDF* obj )
{
  int fd = mNextFD;
  mDescriptorMap[fd] = obj;
  // now look for a new free FD
  do
  {
    mNextFD = mNextFD == kMaxFD ? 0 : mNextFD + 1;
  }
  while( mDescriptorMap.find( mNextFD ) != mDescriptorMap.end() );

  return fd;
}

void XrdFecSys::ReleaseFD( int fd )
{
  if( fd < 0 ) return;
  std::map<int, XrdOssDF*>::iterator it = mDescriptorMap.find( fd );
  if( it == mDescriptorMap.end() ) return;
  mDescriptorMap.erase( it );
}

XrdCl::OpenFlags::Flags XrdFecSys::ToOpenFlags( int flags )
{
  XrdCl::OpenFlags::Flags xflags;

  if( flags & ( O_WRONLY | O_RDWR ) )
      xflags = XrdCl::OpenFlags::Update;
  else
      xflags = XrdCl::OpenFlags::Read;

  if( flags & O_CREAT )
  {
    xflags |= ( flags & O_EXCL ? XrdCl::OpenFlags::New : XrdCl::OpenFlags::Delete );
    xflags |= XrdCl::OpenFlags::MakePath;
  }
  else if( flags & O_TRUNC )
    xflags |= XrdCl::OpenFlags::Delete;

  return xflags;
}

XrdCl::Access::Mode XrdFecSys::ToAccessMode( mode_t mode )
{
  using namespace XrdCl;
  Access::Mode xmode = Access::None;
  if( mode & S_IRUSR ) xmode |= Access::UR;
  if( mode & S_IWUSR ) xmode |= Access::UW;
  if( mode & S_IXUSR ) xmode |= Access::UX;
  if( mode & S_IRGRP ) xmode |= Access::GR;
  if( mode & S_IWGRP ) xmode |= Access::GW;
  if( mode & S_IXGRP ) xmode |= Access::GX;
  if( mode & S_IROTH ) xmode |= Access::OR;
  if( mode & S_IWOTH ) xmode |= Access::OW;
  if( mode & S_IXOTH ) xmode |= Access::OX;
  return xmode;
}

void XrdFecSys::InfoToStat( XrdCl::StatInfo *info, struct stat* buff )
{
  memset( buff, 0, sizeof(struct stat) );
  buff->st_size   = static_cast<size_t>( info->GetSize() );
  buff->st_atime  = buff->st_mtime = buff->st_ctime = static_cast<time_t>( info->GetModTime() );
  buff->st_blocks = buff->st_size / 512 + 1;
  buff->st_ino    = static_cast<ino_t>( strtoll( info->GetId().c_str(), 0, 10 ) );
  buff->st_mode   = XrdPosixMap::Flags2Mode( &buff->st_rdev, info->GetFlags() );
}

