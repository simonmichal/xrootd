/*
 * XrdFecOssDir.cc
 *
 *  Created on: Jun 15, 2016
 *      Author: simonm
 */

#include "XrdFecDir.hh"
#include "XrdFecSys.hh"

XrdFecDir::XrdFecDir( XrdFecSys &sys ) : mSys( sys ), mList( 0 )
{

}

XrdFecDir::~XrdFecDir()
{
  delete mList;
  mSys.ReleaseFD( fd );
}

int XrdFecDir::Opendir( const char *path, XrdOucEnv & )
{
  return -ENOTSUP; // TODO
//  fd = mSys.GetFD( this );
//  std::string realPath = mSys.kPrefix;
//  realPath += path;
//  XrdCl::DirectoryList *list = 0;
//  XrdCl::XRootDStatus st = mSys.mFs.DirList( realPath, XrdCl::DirListFlags::None, list );
//  if( !st.IsOK() )
//  {
//    delete list;
//    return -st.errNo;
//  }
//  if( !list ) return -EBADF;
//
//  mItr = list->Begin();
//  mEnd = list->End();
//
//  return XrdOssOK;
}

int XrdFecDir::Readdir( char *buff, int blen )
{
  return -ENOTSUP; // TODO
//  if( mItr != mEnd )
//  {
//    XrdCl::DirectoryList::ListEntry *entry = *mItr;
//    memcpy( buff, entry->GetName().c_str(), entry->GetName().size() );
//    ++mItr;
//  }
//  else
//    *buff = 0;
//
//  return XrdOssOK;
}

int XrdFecDir::Close( long long *retsz )
{
  delete mList;
  mList = 0;
  mSys.ReleaseFD( fd );
  fd = -1;
  return XrdOssOK;
}

