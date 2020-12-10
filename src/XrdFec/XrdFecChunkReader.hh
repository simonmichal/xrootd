/*
 * XrdFecChunkReader.hh
 *
 *  Created on: Jul 5, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_XRDFECCHUNKREADER_HH_
#define SRC_XRDFEC_XRDFECCHUNKREADER_HH_

#include <queue>
#include <string>

class XrdFecSys;
class XrdFecStripeReader;
namespace XrdCl{ class File; }

class XrdFecChunkReader
{
  public:

    XrdFecChunkReader( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t first, size_t last );

    virtual ~XrdFecChunkReader();

    bool Get( std::string &chunk );

  private:

    XrdFecSys                        &mSys;
    std::vector<XrdCl::File*>        &mFiles;
    size_t                            mFirst;
    size_t                            mLast;
    size_t                            mNext;
    const size_t                      kParallelChunks;
    std::queue<XrdFecStripeReader*>   mChunks;
};

#endif /* SRC_XRDFEC_XRDFECCHUNKREADER_HH_ */
