/*
 * XrdFecChunkWriter.hh
 *
 *  Created on: Jul 12, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_XRDFECCHUNKWRITER_HH_
#define SRC_XRDFEC_XRDFECCHUNKWRITER_HH_

#include <queue>
#include <string>

class XrdFecSys;
class XrdFecStripeWriter;
namespace XrdCl{ class File; }

class XrdFecChunkWriter
{
  public:

    XrdFecChunkWriter( XrdFecSys &sys, std::vector<XrdCl::File*> &files, size_t first, size_t last );

    virtual ~XrdFecChunkWriter();

    void Put( const std::string &chunk );

    void Flush();

  private:

    XrdFecSys                        &mSys;
    std::vector<XrdCl::File*>        &mFiles;
    size_t                            mFirst;
    size_t                            mLast;
    size_t                            mNext;
    const size_t                      kParallelChunks;
    std::queue<XrdFecStripeWriter*>   mChunks;
};

#endif /* SRC_XRDFEC_XRDFECCHUNKWRITER_HH_ */
