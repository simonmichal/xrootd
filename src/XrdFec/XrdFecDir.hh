/*
 * XrdFecOssDir.hh
 *
 *  Created on: Jun 15, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_OLD_XRDFECOSSDIR_HH_
#define SRC_XRDFEC_OLD_XRDFECOSSDIR_HH_

#include "XrdOss/XrdOss.hh"

#include "XrdCl/XrdClXRootDResponses.hh"

class XrdFecSys;

class XrdFecDir : public XrdOssDF
{
  public:

    XrdFecDir( XrdFecSys &sys );

    virtual ~XrdFecDir();

    virtual int Opendir( const char *path, XrdOucEnv & );

    virtual int Readdir( char *buff, int blen );

    virtual int Close( long long *retsz = 0 );

  private:
    XrdFecSys &mSys;
    XrdCl::DirectoryList::Iterator mItr;
    XrdCl::DirectoryList::Iterator mEnd;
    XrdCl::DirectoryList *mList;
};

#endif /* SRC_XRDFEC_OLD_XRDFECOSSDIR_HH_ */
