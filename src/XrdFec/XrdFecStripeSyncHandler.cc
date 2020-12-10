/*
 * XrdFecStripeSyncHandler.cc
 *
 *  Created on: Jul 12, 2016
 *      Author: simonm
 */

#include "XrdFecStripeSyncHandler.hh"

StripeSyncHandler::StripeSyncHandler( size_t size ) : mCond( 0 ), mCounter( size )
{

}

void StripeSyncHandler::HandleResponseWithHosts( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response, XrdCl::HostList *hostList )
{
  delete status;
  delete response;
  delete hostList;
  Release();
}

void StripeSyncHandler::Release()
{
  mCond.Lock();
  --mCounter;
  if( !mCounter )
    mCond.Signal();
  mCond.UnLock();
}

void StripeSyncHandler::Wait()
{
  mCond.Lock();
  while( mCounter ) mCond.Wait();
  mCond.UnLock();
}

