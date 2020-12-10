/*
 * XrdFecChunkReader.cc
 *
 *  Created on: Jul 5, 2016
 *      Author: simonm
 */

#include "XrdFecChunkReader.hh"
#include "XrdFecStripeReader.hh"

XrdFecChunkReader::XrdFecChunkReader( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t first, size_t last ) :
  mSys( sys ), mFiles( files ), mFirst( first ), mLast( last ), mNext( first ), kParallelChunks( 4 )
{

}

XrdFecChunkReader::~XrdFecChunkReader()
{

}

bool XrdFecChunkReader::Get( std::string &chunk )
{
  while( mChunks.size() < kParallelChunks && mNext <= mLast )
  {
    XrdFecStripeReader *reader = new XrdFecStripeReader( mSys, mFiles, mNext );
    reader->AsyncRead();
    mChunks.push( reader );
    ++mNext;
  }

  if( mChunks.empty() )
    return false;

  std::unique_ptr<XrdFecStripeReader> ptr( mChunks.front() );
  mChunks.pop();
  ptr->Wait();
  chunk = ptr->GetChunk();
  return true;
}
