/*
 * X	rdFecStripeWriter.cc
 *
 *  Created on: Jul 11, 2016
 *      Author: simonm
 */

#include "XrdFecStripeWriter.hh"
#include "XrdFecSys.hh"
#include "XrdFecRedundancyProvider.hh"

#include "XrdCl/XrdClFile.hh"


const size_t XrdFecStripeWriter::k4KB = 4 * 1024;

XrdFecStripeWriter::XrdFecStripeWriter( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t index ) :
    mFiles( files ), mChunkIndex( index ), mSyncHandler( sys.kNbData + sys.kNbParity ), kChunkSize( sys.kNbData * k4KB ), kNbData( sys.kNbData ), kNbParity( sys.kNbParity )
{

}

XrdFecStripeWriter::~XrdFecStripeWriter()
{

}

XrdFecStripeWriter::Stripes XrdFecStripeWriter::MakeStripes( const std::string &chunk )
{
  std::vector< std::shared_ptr<const std::string> > stripes;
  for( size_t i = 0; i < kNbData + kNbParity; ++i )
  {
    if( i < kNbData )
    {
      std::string stripe = chunk.substr( i * k4KB, k4KB );
      stripes.push_back( std::make_shared<const std::string>( std::move( stripe ) ) );
    }
    else
      stripes.push_back( nullptr );
  }
  return stripes;
}


void XrdFecStripeWriter::PutChunk( const std::string &chunk )
{
  mStripes = MakeStripes( chunk );
  RedundancyProvider redundancy( kNbData, kNbParity );
  redundancy.compute( mStripes );

  size_t stripeIndex = 0;
  for( auto &stripe : mStripes )
  {
    if( !WriteStripe( stripeIndex, *stripe ) ) mSyncHandler.Release(); // TODO we need some logic to handle partial write, also make sure you are updating the right version !!!
    ++stripeIndex;
  }
}

void XrdFecStripeWriter::Wait()
{
  mSyncHandler.Wait();
}

bool XrdFecStripeWriter::WriteStripe( size_t stripeIndex, const std::string &stripe )
{
  if( stripe.size() != k4KB ) throw std::length_error( "Wrong stripe size." );
  auto fptr = mFiles[stripeIndex];
  off_t  offset = mChunkIndex * k4KB;
  size_t blen   = k4KB;
  XrdCl::XRootDStatus st = fptr->Write( offset, blen, stripe.c_str(), &mSyncHandler ); // TODO we need some logic to handle partial write, also make sure you are updating the right version !!!
  return st.IsOK();
}
