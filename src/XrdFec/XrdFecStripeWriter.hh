/*
 * XrdFecStripeWriter.hh
 *
 *  Created on: Jul 11, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_XRDFECSTRIPEWRITER_HH_
#define SRC_XRDFEC_XRDFECSTRIPEWRITER_HH_

#include <string>

#include "XrdFecStripeSyncHandler.hh"

class XrdFecSys;
namespace XrdCl{ class File; };

class XrdFecStripeWriter
{
  public:

    XrdFecStripeWriter( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t index );

    virtual ~XrdFecStripeWriter();

    void PutChunk( const std::string &chunk );

    void Wait();

  private:

    typedef std::vector< std::shared_ptr<const std::string> > Stripes;

    bool WriteStripe( size_t stripeIndex, const std::string & stripe );

    Stripes MakeStripes( const std::string &chunk );

    std::vector<XrdCl::File*> &mFiles;
    const size_t               mChunkIndex;
    StripeSyncHandler          mSyncHandler;
    Stripes mStripes;

  public:

    static const size_t k4KB;
    const size_t kChunkSize;
    const size_t kNbData;
    const size_t kNbParity;
};

#endif /* SRC_XRDFEC_XRDFECSTRIPEWRITER_HH_ */
