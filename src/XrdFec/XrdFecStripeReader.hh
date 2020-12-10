/*
 * XrdFecStripeReader.hh
 *
 *  Created on: Jul 4, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_XRDFECSTRIPEREADER_HH_
#define SRC_XRDFEC_XRDFECSTRIPEREADER_HH_


#include <memory>
#include <vector>
#include <string>

#include "XrdFecStripeSyncHandler.hh"

#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdSys/XrdSysPthread.hh"



class XrdFecSys;
namespace XrdCl{ class File; };

class XrdFecStripeReader
{
  public:
    XrdFecStripeReader( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t index );

    virtual ~XrdFecStripeReader();

    void AsyncRead();

    void Wait();

    std::string GetChunk();

  private:

    typedef std::vector< std::shared_ptr<const std::string> > Stripes;

    bool ReadStripe( size_t stripeIndex );

    std::vector<XrdCl::File*> &mFiles;
    const size_t               mChunkIndex;
    size_t                     mErrCount;
    Stripes                    mStripes;
    StripeSyncHandler          mSyncHandler;

  public:

    static const size_t k4KB;

    const size_t kChunkSize;
    const size_t kNbData;
    const size_t kNbParity;
};


#endif /* SRC_XRDFEC_XRDFECSTRIPEREADER_HH_ */
