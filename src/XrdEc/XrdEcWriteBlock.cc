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

#include <memory>
#include <mutex>
#include <atomic>

namespace
{
  using namespace XrdEc;

  struct StrmWrtCtx
  {
      StrmWrtCtx( WrtBuff                &&wrtbuff,
                  const std::string      &&objname,
                  const placement_group  &plgr,
                  const placement_t      &&placement,
                  uint64_t                fnlsize,
                  XrdCl::ResponseHandler  *handler ) : wrtbuff( std::move( wrtbuff ) ),
                                                      objname( objname ),
                                                      placement( placement ),
                                                      spares( GetSpares( plgr, placement, true ) ),
                                                      fnlsize( fnlsize ),
                                                      handler( handler )
      {
      }

      ~StrmWrtCtx()
      {
        XrdCl::ResponseHandler* hdlr = handler.exchange( nullptr );
        if( hdlr ) hdlr->HandleResponse( new XrdCl::XRootDStatus(), new XrdCl::AnyObject() );
      }

      bool Retry( XrdCl::XRootDStatus &st, uint8_t strpnb )
      {
        std::unique_lock<std::mutex> lck( mtx );
        if( st.IsOK() || spares.empty() ) return false;
        placement[strpnb] = spares.back();
        spares.pop_back();
        return true;
      }

      void Handle( XrdCl::XRootDStatus &st, uint8_t strpnb )
      {
        if( !st.IsOK() )
        {
          XrdCl::ResponseHandler* hdlr = handler.exchange( nullptr );
          if( hdlr ) hdlr->HandleResponse( new XrdCl::XRootDStatus( st ), new XrdCl::AnyObject() );
        }
      }

      WrtBuff                              wrtbuff;
      const std::string                    objname;
      placement_t                          placement;
      placement_t                          spares;
      uint64_t                             fnlsize;
      std::atomic<XrdCl::ResponseHandler*> handler;
      std::mutex                           mtx;
  };

  static void WriteStripe( uint8_t                      strpnb,
                           std::shared_ptr<StrmWrtCtx>  ctx    )
  {
    using namespace XrdCl;

    std::unique_lock<std::mutex> lck( ctx->mtx );
    std::shared_ptr<File> file( new File() );
    XrdCl::OpenFlags::Flags flags = OpenFlags::Write | OpenFlags::New | OpenFlags::POSC;
    std::string url = ctx->placement[strpnb] + '/' + ctx->objname;
    std::string checksum = ctx->wrtbuff.GetChecksum( strpnb );

    // Construct the pipeline
    Pipeline wrtstrp = Open( file.get(), url, flags )
                     | Parallel( Write( file.get(), 0, ctx->wrtbuff.GetStrpSize( strpnb ), ctx->wrtbuff.GetChunk( strpnb ) ),
                                 SetXAttr( file.get(), "xrdec.checksum", checksum ),
                                 SetXAttr( file.get(), "xrdec.fnlsize", std::to_string( ctx->fnlsize ) ),
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
}

namespace XrdEc
{

  void WriteBlock( const std::string      &obj,
                   const std::string      &sign,
                   const placement_group  &plgr,
                   uint64_t                fnlsize,
                   WrtBuff               &&wrtbuff,
                   XrdCl::ResponseHandler *handler )
  {
    std::string objname = obj + '.' + std::to_string( wrtbuff.GetBlkNb() ) + '?' + "ost.sig=" + sign;
    placement_t placement = GeneratePlacement( objname, plgr, true );

    std::shared_ptr<StrmWrtCtx> ctx( new StrmWrtCtx( std::move( wrtbuff ),
                                                     std::move( objname ),
                                                     plgr,
                                                     std::move( placement ),
                                                     fnlsize,
                                                     handler ) );

    Config &cfg = Config::Instance();
    for( uint8_t strpnb = 0; strpnb < cfg.nbchunks; ++strpnb )
      WriteStripe( strpnb, ctx );
  }

} /* namespace XrdEc */
