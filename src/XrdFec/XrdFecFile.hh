/*
 * XrdFecOssFile.hh
 *
 *  Created on: Jun 15, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_OLD_XRDFECOSSFILE_HH_
#define SRC_XRDFEC_OLD_XRDFECOSSFILE_HH_

#include "XrdOss/XrdOss.hh"
#include "XrdCl/XrdClFile.hh"

#include <XrdFecRedundancyProvider.hh>

#include <vector>

class XrdFecSys;

class XrdFecFile : public XrdOssDF
{
  public:
    XrdFecFile( XrdFecSys &sys );

    virtual ~XrdFecFile();

    virtual int Open( const char *path, int flags, mode_t mode, XrdOucEnv &env );

    virtual int Close( long long *retsz = 0 );

    virtual ssize_t Read( off_t offset, size_t blen );

    virtual ssize_t Read( void *buff, off_t offset, size_t blen );

    virtual int Read( XrdSfsAio *aoip );

    virtual ssize_t ReadRaw( void *buff, off_t offset, size_t blen );

    virtual int Fstat( struct stat *buff );

    virtual ssize_t Write( const void *buff, off_t offset, size_t blen );

    virtual int Write( XrdSfsAio *aiop );

    virtual int Fsync();

    virtual int Ftruncate( unsigned long long size );

  private:

    void CleanFiles();

    typedef std::vector<std::shared_ptr<const std::string> > Stripes;

    size_t FirstChunk( off_t offset, off_t &begin );

    size_t LastChunk( off_t offset, size_t blen );

    void PadChunk( std::string & chunk );

    std::vector<XrdCl::File*> mFiles;
    XrdFecSys &mSys;
    RedundancyProvider mRedundancy;
    std::string mBuff;

    static const size_t k4KB;
    const size_t kChunkSize;
};

#endif /* SRC_XRDFEC_OLD_XRDFECOSSFILE_HH_ */
