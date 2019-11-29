/*
 * XrdEcStrmWriter.cc
 *
 *  Created on: Oct 14, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcWriteBlock.hh"
#include "XrdEc/XrdEcWrtBuff.hh"
#include "XrdCl/XrdClFileOperations.hh"
#include "XrdCl/XrdClParallelOperation.hh"
#include "XrdEc/XrdEcLogger.hh"

#include <memory>
#include <mutex>
#include <atomic>
#include <algorithm>

namespace
{
  using namespace XrdEc;

  struct StrmWrtCtxBase
  {
      StrmWrtCtxBase( const ObjCfg           &objcfg,
                      const std::string      &&blkname,
                      const placement_group  &plgr,
                      const placement_t      &&placement,
                      XrdCl::ResponseHandler  *handler ) : objcfg( objcfg ),
                                                           blkname( std::move( blkname ) ),
                                                           placement( std::move( placement ) ),
                                                           spares( GetSpares( plgr, placement, true ) ),
                                                           handler( handler )
      {
        retriable.reserve( objcfg.nbchunks );
        std::fill( retriable.begin(), retriable.end(), true );
      }

      virtual ~StrmWrtCtxBase()
      {
        XrdCl::ResponseHandler* hdlr = handler.exchange( nullptr );
        if( hdlr ) hdlr->HandleResponse( new XrdCl::XRootDStatus(), nullptr );
      }

      bool Retry( XrdCl::XRootDStatus &st, uint8_t strpnb )
      {
        std::unique_lock<std::mutex> lck( mtx );

        Logger &log = Logger::Instance();
        std::stringstream ss;
        ss << "StrmWrtCtxBase::Retry (" << (void*)this << ") : url = " << ( placement[strpnb] + '/' + blkname ) << ", st = " << st.ToString();

        // if everything is OK or there are no spares we don't retry
        if( st.IsOK() || !retriable[strpnb] || spares.empty() )
        {
          ss << ", return = false";
          log.Entry( ss.str() );
          return false;
        }

        ss << ", return = true";
        log.Entry( ss.str() );

        placement[strpnb] = spares.back();
        spares.pop_back();
        return true;
      }

      void Handle( XrdCl::XRootDStatus &st, uint8_t strpnb )
      {
        if( !st.IsOK() )
        {
          XrdCl::ResponseHandler* hdlr = handler.exchange( nullptr );
          if( hdlr ) hdlr->HandleResponse( new XrdCl::XRootDStatus( st ), nullptr );
        }
      }

      void HandleOpenStatus( const XrdCl::XRootDStatus &st, uint8_t strpnb )
      {
        std::unique_lock<std::mutex> lck( mtx );
        retriable[strpnb] = !( !st.IsOK() && st.code == XrdCl::errErrorResponse &&
                               ( st.errNo == kXR_InvalidRequest || st.errNo == kXR_FileLocked ) ) ;
      }

      ObjCfg                               objcfg;
      std::string                          blkname;
      placement_t                          placement;
      placement_t                          spares;
      std::atomic<XrdCl::ResponseHandler*> handler;
      std::vector<bool>                    retriable;
      std::mutex                           mtx;
  };

  struct StrmWrtCtx : public StrmWrtCtxBase
  {
      StrmWrtCtx( WrtBuff                &&wrtbuff,
                  const ObjCfg           &objcfg,
                  const std::string      &&objname,
                  const placement_group  &plgr,
                  const placement_t      &&placement,
                  XrdCl::ResponseHandler  *handler ) : StrmWrtCtxBase( objcfg, std::move( objname) , plgr, std::move( placement ), handler ),
                                                       wrtbuff( std::move( wrtbuff ) )
      {
      }

      WrtBuff                              wrtbuff;
  };

  static void WriteStripe( const ObjCfg                &objcfg,
                           uint8_t                      strpnb,
                           std::shared_ptr<StrmWrtCtx>  ctx    )
  {
    using namespace XrdCl;

    std::unique_lock<std::mutex> lck( ctx->mtx );
    std::shared_ptr<File> file( new File( false ) );
    XrdCl::OpenFlags::Flags flags = ctx->wrtbuff.GetWrtMode() == WrtMode::New ? OpenFlags::New : OpenFlags::Delete;
    flags |= OpenFlags::Write | OpenFlags::POSC;
    std::string url = ctx->placement[strpnb] + '/' + ctx->blkname;
    std::string checksum = ctx->wrtbuff.GetChecksum( strpnb );

    std::stringstream ss;
    ss << "WriteStripe (StrmWrtCtx = " << ctx.get() << ") : url = " << url;
    Logger &log = Logger::Instance();
    log.Entry( ss.str() );

    // Construct the pipeline
    Pipeline wrtstrp = Open( file.get(), url, flags ) >> [strpnb, ctx]( XRootDStatus &st ){ ctx->HandleOpenStatus( st, strpnb ); }
                     | Parallel( Write( file.get(), 0, ctx->wrtbuff.GetStrpSize( strpnb ), ctx->wrtbuff.GetChunk( strpnb ) ),
                                 SetXAttr( file.get(), "xrdec.checksum", checksum ),
                                 SetXAttr( file.get(), "xrdec.blksize", std::to_string( ctx->wrtbuff.GetBlkSize() ) ),
                                 SetXAttr( file.get(), "xrdec.strpnb", std::to_string( strpnb ) ) )
                     | Close( file.get() ) >> [file, strpnb, ctx, objcfg]( XRootDStatus &st )
                         {
                           if( ctx->Retry( st, strpnb ) )
                             WriteStripe( objcfg, strpnb, ctx );
                           else
                             ctx->Handle( st, strpnb );
                         };

    // Run the pipeline
    Async( std::move( wrtstrp ) );
  }

  std::string GetChecksum( const ObjCfg &objcfg )
  {
    using namespace XrdCl;

    CheckSumManager *cksMan = DefaultEnv::GetCheckSumManager();
    XrdCksCalc *cksCalcObj = cksMan->GetCalculator( "zcrc32" );
    char buffer[objcfg.chunksize];
    memset( buffer, 0, objcfg.chunksize );
    cksCalcObj->Update( buffer, objcfg.chunksize );

    int          calcSize = 0;
    std::string  calcType = cksCalcObj->Type( calcSize );

    XrdCksData ckSum;
    ckSum.Set( calcType.c_str() );
    ckSum.Set( (void*)cksCalcObj->Final(), calcSize );
    char *cksBuffer = new char[265];
    ckSum.Get( cksBuffer, 256 );
    std::string checkSum  = calcType + ":";
    checkSum += Utils::NormalizeChecksum( calcType, cksBuffer );
    delete [] cksBuffer;
    delete cksCalcObj;
    return checkSum;
  }

  static void WriteEmptyStripe( const ObjCfg &objcfg,
                                uint8_t strpnb,
                                std::shared_ptr<StrmWrtCtxBase> ctx )
  {
    using namespace XrdCl;

    std::shared_ptr<File> file( new File( false ) );
    OpenFlags::Flags flags = OpenFlags::New | OpenFlags::Write | OpenFlags::POSC;
    std::string url = ctx->placement[strpnb] + '/' + ctx->blkname;
    static const std::string checksum = GetChecksum( objcfg );

    std::stringstream ss;
    ss << "WriteEmptyStripe (StrmWrtCtxBase = " << ctx.get() << ") : url = " << url;
    Logger &log = Logger::Instance();
    log.Entry( ss.str() );

    // Construct the pipeline
    Pipeline wrtstrp = Open( file.get(), url, flags ) >> [strpnb, ctx]( XRootDStatus &st ){ ctx->HandleOpenStatus( st, strpnb ); }
                     | Parallel( SetXAttr( file.get(), "xrdec.checksum", checksum ),
                                 SetXAttr( file.get(), "xrdec.blksize", std::to_string( objcfg.datasize ) ),
                                 SetXAttr( file.get(), "xrdec.strpnb", std::to_string( strpnb ) ) )
                     | Close( file.get() ) >> [file, strpnb, ctx, objcfg]( XRootDStatus &st )
                         {
                           if( ctx->Retry( st, strpnb ) )
                             WriteEmptyStripe( objcfg, strpnb, ctx );
                           else
                             ctx->Handle( st, strpnb );
                         };

    // Run the pipeline
    Async( std::move( wrtstrp ) );
  }
}

namespace XrdEc
{

  void WriteBlock( const ObjCfg           &objcfg,
                   const std::string      &sign,
                   const placement_group  &plgr,
                   WrtBuff               &&wrtbuff,
                   XrdCl::ResponseHandler *handler )
  {
    uint64_t blknb = wrtbuff.GetBlkNb();
    std::string blkname = objcfg.obj + '.' + std::to_string( blknb );
    placement_t placement = GeneratePlacement( blkname, blkname, plgr, true );
    blkname += "?ost.sig=" + sign;

    std::shared_ptr<StrmWrtCtx> ctx( new StrmWrtCtx( std::move( wrtbuff ),
                                                     objcfg,
                                                     std::move( blkname ),
                                                     plgr,
                                                     std::move( placement ),
                                                     handler ) );

    for( uint8_t strpnb = 0; strpnb < objcfg.nbchunks; ++strpnb )
      WriteStripe( objcfg, strpnb, ctx );
  }

  void CreateEmptyBlock( const ObjCfg           &objcfg,
                         const std::string      &sign,
                         const placement_group  &plgr,
                         uint64_t                blknb,
                         XrdCl::ResponseHandler *handler )
  {
    std::string blkname = objcfg.obj + '.' + std::to_string( blknb );
    placement_t placement = GeneratePlacement( blkname, blkname, plgr, true );
    blkname += "?ost.sig=" + sign;

    std::shared_ptr<StrmWrtCtxBase> ctx( new StrmWrtCtxBase( objcfg,
                                                             std::move( blkname ),
                                                             plgr,
                                                             std::move( placement ),
                                                             handler ) );
    for( uint8_t strpnb = 0; strpnb < objcfg.nbchunks; ++strpnb )
      WriteEmptyStripe( objcfg, strpnb, ctx );
  }

} /* namespace XrdEc */
