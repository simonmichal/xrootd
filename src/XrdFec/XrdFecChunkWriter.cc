/*
 * XrdFecChunkWriter.cc
 *
 *  Created on: Jul 12, 2016
 *      Author: simonm
 */

#include "XrdFecChunkWriter.hh"
#include "XrdFecStripeWriter.hh"

XrdFecChunkWriter::XrdFecChunkWriter( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t first, size_t last ) :
  mSys( sys ), mFiles( files ), mFirst( first ), mLast( last ), mNext( first ), kParallelChunks( 1 )
{
  // TODO Auto-generated constructor stub

}

XrdFecChunkWriter::~XrdFecChunkWriter()
{
  // TODO Auto-generated destructor stub
}

void XrdFecChunkWriter::Put( const std::string &chunk )
{
  if( mChunks.size() == kParallelChunks )
  {
    std::unique_ptr<XrdFecStripeWriter> ptr( mChunks.front() );
    mChunks.pop();
    ptr->Wait();
  }

  XrdFecStripeWriter *writer = new XrdFecStripeWriter( mSys, mFiles, mNext );
  writer->PutChunk( chunk );
  mChunks.push( writer );
  ++mNext;
}

void XrdFecChunkWriter::Flush()
{
  while( !mChunks.empty() )
  {
    std::unique_ptr<XrdFecStripeWriter> ptr( mChunks.front() );
    mChunks.pop();
    ptr->Wait();
  }
}

