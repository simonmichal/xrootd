/*
 * XrdFecOss.hh
 *
 *  Created on: Jun 15, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_OLD_XRDFECOSS_HH_
#define SRC_XRDFEC_OLD_XRDFECOSS_HH_

#include <XrdOss/XrdOss.hh>
#include <XrdCl/XrdClFileSystem.hh>

#include <map>

class XrdFecSys : public XrdOss
{
  public:
    XrdFecSys();
    virtual ~XrdFecSys();

    virtual XrdOssDF* newDir(const char *tident);

    virtual XrdOssDF* newFile(const char *tident);

    virtual int Chmod(const char *, mode_t mode, XrdOucEnv *eP=0);

    virtual int Create(const char *, const char *, mode_t, XrdOucEnv &, int opts=0);

    virtual int Init(XrdSysLogger *, const char *);

    virtual int Mkdir(const char *, mode_t mode, int mkpath=0, XrdOucEnv *eP=0);

    virtual int Remdir(const char *, int Opts=0, XrdOucEnv *eP=0);

    virtual int Rename(const char *, const char *, XrdOucEnv *eP1=0, XrdOucEnv *eP2=0);

    virtual int Stat(const char *, struct stat *, int opts=0, XrdOucEnv *eP=0);

    virtual int Truncate(const char *, unsigned long long, XrdOucEnv *eP=0);

    virtual int Unlink(const char *, int Opts=0, XrdOucEnv *eP=0);

    int GetFD( XrdOssDF* obj );

    void ReleaseFD( int fd );

    static XrdCl::OpenFlags::Flags ToOpenFlags( int flags );

    static XrdCl::Access::Mode ToAccessMode( mode_t mode );

    static void InfoToStat( XrdCl::StatInfo *info, struct stat* buff );

    std::vector<std::string> mPrefixes;
    std::vector<std::string> mHostIDs;
    std::vector<XrdCl::FileSystem*> mFSs;

  private:
    std::map<int, XrdOssDF*> mDescriptorMap;
    static const int kMinFD;
    static const int kMaxFD;
    int mNextFD;

  public:
    const size_t kNbData;
    const size_t kNbParity;
};


#endif /* SRC_XRDFEC_OLD_XRDFECOSS_HH_ */
