/*
 * XrdEcStrmWriter.hh
 *
 *  Created on: Oct 14, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECWRITEBLOCK_HH_
#define SRC_XRDEC_XRDECWRITEBLOCK_HH_

#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdEc/XrdEcUtilities.hh"

namespace XrdEc
{
  class WrtBuff;

  void WriteBlock( const ObjCfg           &objcfg,
                   const std::string      &sign,
                   const placement_group  &plgr,
                   WrtBuff               &&wrtbuff,
                   XrdCl::ResponseHandler *handler );

  void CreateEmptyBlock( const ObjCfg           &objcfg,
                         const std::string      &sign,
                         const placement_group  &plgr,
                         uint64_t                blknb,
                         XrdCl::ResponseHandler *handler );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECWRITEBLOCK_HH_ */
