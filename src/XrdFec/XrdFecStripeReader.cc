/*
 * XrdFecStripeReader.cc
 *
 *  Created on: Jul 4, 2016
 *      Author: simonm
 */

#include "XrdFecStripeReader.hh"

#include "XrdFecSys.hh"
#include "XrdFecRedundancyProvider.hh"

#include "XrdCl/XrdClFile.hh"

#include <algorithm>
#include <iterator>


class StripeReadHandler : public XrdCl::ResponseHandler
{
  public:

    StripeReadHandler( XrdCl::File *fptr, XrdCl::ResponseHandler *userHandler, std::shared_ptr<const std::string> &stripe ) :
      pFile( fptr ), mBuff( XrdFecStripeReader::k4KB, '\0' ), pUserHandler( userHandler ), mStripe( stripe ), pBegin( mBuff.data() ), mSize( XrdFecStripeReader::k4KB )
    {

    }

    //----------------------------------------------------------------------------
    // Handle the response
    //----------------------------------------------------------------------------
    virtual void HandleResponseWithHosts( XrdCl::XRootDStatus *status, XrdCl::AnyObject *response, XrdCl::HostList *hostList )
    {
      try
      {
        if( !status->IsOK() ) throw std::exception();
        // make sure we got a response
        if( !response ) throw std::exception();
        // also make sure that we got the right response
        XrdCl::ChunkInfo * info = 0;
        response->Get( info );
        if( !info ) throw std::exception();
        // update the state
        uint32_t bytesRead  = info->length;
        uint64_t offset     = info->offset + bytesRead;
        pBegin             += bytesRead;
        mSize              -= bytesRead;
        // are we done ?
        if( bytesRead == 0 )
        {
          std::string stripe; stripe.reserve( XrdFecStripeReader::k4KB );
          std::copy( mBuff.begin(), mBuff.end(), std::back_inserter( stripe ) );
          mStripe = std::make_shared<std::string>( std::move( stripe ) );
          pUserHandler->HandleResponseWithHosts( status, response, hostList );
          delete this;
        }
        else
        {
          auto st = pFile->Read( offset, mSize, pBegin, this );
          if( !st.IsOK() ) throw std::exception();
          // clean up
          delete status;
          delete response;
          delete hostList;
        }
      }
      catch( std::exception &ex )
      {
        pUserHandler->HandleResponseWithHosts( status, response, hostList );
        delete this;
      }
    }

    char* GetBuff()
    {
      return pBegin;
    }

  private:
    XrdCl::File                        *pFile;
    std::vector<char>                   mBuff;
    XrdCl::ResponseHandler             *pUserHandler;
    std::shared_ptr<const std::string> &mStripe;
    char                               *pBegin;
    size_t                              mSize;
};


const size_t XrdFecStripeReader::k4KB = 4 * 1024;

XrdFecStripeReader::XrdFecStripeReader( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t index ) :
    mFiles( files ), mChunkIndex( index ), mErrCount( 0 ), mStripes( sys.kNbData + sys.kNbParity, nullptr ), mSyncHandler( sys.kNbData + sys.kNbParity ), kChunkSize( sys.kNbData * k4KB ), kNbData( sys.kNbData ), kNbParity( sys.kNbParity )
{

}

XrdFecStripeReader::~XrdFecStripeReader()
{

}

void XrdFecStripeReader::AsyncRead()
{
  size_t size = kNbData + kNbParity;
  for( size_t stripeIndex = 0; stripeIndex != size; ++ stripeIndex )
  {
    if( !ReadStripe( stripeIndex ) ) mSyncHandler.Release();
  }
}

void XrdFecStripeReader::Wait()
{
  mSyncHandler.Wait();
  RedundancyProvider redundancy( kNbData, kNbParity ); // TODO later on make sure this is done in the threadpool
  redundancy.compute( mStripes );
}

std::string XrdFecStripeReader::GetChunk()
{
  // concatenate the stripes into a single chunk
  std::string chunk; chunk.reserve( kChunkSize );
  for( size_t i = 0; i < kNbData; ++i )
  {
    if( !mStripes[i] ) return std::string();
    chunk += *mStripes[i];
  }

  return chunk;
}


bool XrdFecStripeReader::ReadStripe( size_t stripeIndex )
{
  auto               fptr    = mFiles[stripeIndex];
  off_t              offset  = mChunkIndex * k4KB;
  StripeReadHandler *handler = new StripeReadHandler( fptr, &mSyncHandler, mStripes[stripeIndex] );
  char              *buff    = handler->GetBuff();
  auto st = fptr->Read( offset, k4KB, buff, handler );
  if( !st.IsOK() ) delete handler;
  return st.IsOK();
}
