/*
 * XrdEcStrmWriter.cc
 *
 *  Created on: Oct 14, 2019
 *      Author: simonm
 */

#include "XrdEc/XrdEcWriteBlock.hh"
#include "XrdEc/XrdEcConfig.hh"
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
      StrmWrtCtxBase( const std::string      &&objname,
                      const placement_group  &plgr,
                      const placement_t      &&placement,
                      XrdCl::ResponseHandler  *handler ) : objname( std::move( objname ) ),
                                                           placement( std::move( placement ) ),
                                                           spares( GetSpares( plgr, placement, true ) ),
                                                           handler( handler )
      {
        Config &cfg = Config::Instance();
        retriable.reserve( cfg.nbchunks );
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
        ss << "StrmWrtCtxBase::Retry (" << (void*)this << ") : url = " << ( placement[strpnb] + objname );

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

      const std::string                    objname;
      placement_t                          placement;
      placement_t                          spares;
      std::atomic<XrdCl::ResponseHandler*> handler;
      std::vector<bool>                    retriable;
      std::mutex                           mtx;
  };

  struct StrmWrtCtx : public StrmWrtCtxBase
  {
      StrmWrtCtx( WrtBuff                &&wrtbuff,
                  const std::string      &&objname,
                  const placement_group  &plgr,
                  const placement_t      &&placement,
                  XrdCl::ResponseHandler  *handler ) : StrmWrtCtxBase( std::move( objname) , plgr, std::move( placement ), handler ),
                                                       wrtbuff( std::move( wrtbuff ) )
      {
      }

      WrtBuff                              wrtbuff;
  };

  static void WriteStripe( uint8_t                      strpnb,
                           std::shared_ptr<StrmWrtCtx>  ctx    )
  {
    using namespace XrdCl;

    std::unique_lock<std::mutex> lck( ctx->mtx );
    std::shared_ptr<File> file( new File() );
    XrdCl::OpenFlags::Flags flags = ctx->wrtbuff.GetWrtMode() == WrtMode::New ? OpenFlags::New : OpenFlags::Delete;
    flags |= OpenFlags::Write | OpenFlags::POSC;
    std::string url = ctx->placement[strpnb] + '/' + ctx->objname;
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
                     | Close( file.get() ) >> [file, strpnb, ctx]( XRootDStatus &st )
                         {
                           if( ctx->Retry( st, strpnb ) )
                             WriteStripe( strpnb, ctx );
                           else
                             ctx->Handle( st, strpnb );
                         };

    // Run the pipeline
    Async( std::move( wrtstrp ) );
  }

  std::string GetChecksum()
  {
    using namespace XrdCl;

    CheckSumManager *cksMan = DefaultEnv::GetCheckSumManager();
    XrdCksCalc *cksCalcObj = cksMan->GetCalculator( "zcrc32" );
    Config &cfg = Config::Instance();
    char buffer[cfg.chunksize];
    memset( buffer, 0, cfg.chunksize );
    cksCalcObj->Update( buffer, cfg.chunksize );

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

  static void WriteEmptyStripe( uint8_t strpnb,
                                std::shared_ptr<StrmWrtCtxBase> ctx )
  {
    using namespace XrdCl;

    std::shared_ptr<File> file( new File() );
    OpenFlags::Flags flags = OpenFlags::New | OpenFlags::Write | OpenFlags::POSC;
    std::string url = ctx->placement[strpnb] + '/' + ctx->objname;
    static const std::string checksum = GetChecksum();

    std::stringstream ss;
    ss << "WriteEmptyStripe (StrmWrtCtxBase = " << ctx.get() << ") : url = " << url;
    Logger &log = Logger::Instance();
    log.Entry( ss.str() );

    Config &cfg = Config::Instance();

    // Construct the pipeline
    Pipeline wrtstrp = Open( file.get(), url, flags ) >> [strpnb, ctx]( XRootDStatus &st ){ ctx->HandleOpenStatus( st, strpnb ); }
                     | Parallel( SetXAttr( file.get(), "xrdec.checksum", checksum ),
                                 SetXAttr( file.get(), "xrdec.blksize", std::to_string( cfg.datasize ) ),
                                 SetXAttr( file.get(), "xrdec.strpnb", std::to_string( strpnb ) ) )
                     | Close( file.get() ) >> [file, strpnb, ctx]( XRootDStatus &st )
                         {
                           if( ctx->Retry( st, strpnb ) )
                             WriteEmptyStripe( strpnb, ctx );
                           else
                             ctx->Handle( st, strpnb );
                         };

    // Run the pipeline
    Async( std::move( wrtstrp ) );
  }
}

namespace XrdEc
{

  void WriteBlock( const std::string      &obj,
                   const std::string      &sign,
                   const placement_group  &plgr,
                   WrtBuff               &&wrtbuff,
                   XrdCl::ResponseHandler *handler )
  {
    std::string objname = obj + '.' + std::to_string( wrtbuff.GetBlkNb() ) + '?' + "ost.sig=" + sign;
    placement_t placement = GeneratePlacement( objname, plgr, true );

    std::shared_ptr<StrmWrtCtx> ctx( new StrmWrtCtx( std::move( wrtbuff ),
                                                     std::move( objname ),
                                                     plgr,
                                                     std::move( placement ),
                                                     handler ) );

    Config &cfg = Config::Instance();
    for( uint8_t strpnb = 0; strpnb < cfg.nbchunks; ++strpnb )
      WriteStripe( strpnb, ctx );
  }

  void CreateEmptyBlock( const std::string      &obj,
                         const std::string      &sign,
                         const placement_group  &plgr,
                         uint64_t                blknb,
                         XrdCl::ResponseHandler *handler )
  {
    std::string objname = obj + '.' + std::to_string( blknb ) + '?' + "ost.sig=" + sign;
    placement_t placement = GeneratePlacement( objname, plgr, true );

    std::shared_ptr<StrmWrtCtxBase> ctx( new StrmWrtCtxBase( std::move( objname ),
                                                             plgr,
                                                             std::move( placement ),
                                                             handler ) );

    Config &cfg = Config::Instance();
    for( uint8_t strpnb = 0; strpnb < cfg.nbchunks; ++strpnb )
      WriteEmptyStripe( strpnb, ctx );
  }

} /* namespace XrdEc */
