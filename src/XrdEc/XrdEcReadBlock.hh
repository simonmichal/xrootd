/*
 * XrdEcStreamRead.hh
 *
 *  Created on: Oct 18, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECREADBLOCK_HH_
#define SRC_XRDEC_XRDECREADBLOCK_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcObjCfg.hh"
#include "XrdEc/XrdEcCallbackWrapper.hh"

#include <memory>
#include <string>

namespace XrdCl{ class ResponseHandler; }

namespace XrdEc
{
  class RdBuff;

  std::shared_ptr<CallbackWrapper> GetRdHandler( const ObjCfg &objcfg, uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler );

  bool BlockAligned( const ObjCfg &objcfg, uint64_t offset, uint32_t size );

  void ReadBlock( const ObjCfg                     &objcfg,
                  const std::string                &sign,
                  const placement_group            &plgr,
                  uint64_t                          offset,
                  char                             *buffer,
                  std::shared_ptr<CallbackWrapper> &callback ); // buffer big enough to accommodate full block

  void ReadBlock( const ObjCfg                     &objcfg,
                  const std::string                &sign,
                  const placement_group            &plgr,
                  std::shared_ptr<RdBuff>          &rdbuff,
                  std::shared_ptr<CallbackWrapper> &callback );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECREADBLOCK_HH_ */
