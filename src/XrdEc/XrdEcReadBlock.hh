/*
 * XrdEcStreamRead.hh
 *
 *  Created on: Oct 18, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECREADBLOCK_HH_
#define SRC_XRDEC_XRDECREADBLOCK_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include "XrdEc/XrdEcConfig.hh"

#include <memory>
#include <string>

namespace XrdCl{ class ResponseHandler; }

namespace XrdEc
{
  class RdBuff;

  XrdCl::ResponseHandler* GetRdHandler( uint64_t offset, uint32_t size, char *buffer, XrdCl::ResponseHandler *handler );

  bool BlockAligned( uint64_t offset, uint32_t size );

  void ReadBlock( const std::string      &obj,
                  const std::string      &sign,
                  const placement_group  &plgr,
                  uint64_t                offset,
                  char                   *buffer,
                  XrdCl::ResponseHandler *handler ); // buffer big enough to accommodate full block

  void ReadBlock( const std::string       &obj,
                  const std::string       &sign,
                  const placement_group   &plgr,
                  std::shared_ptr<RdBuff> &rdbuff,
                  XrdCl::ResponseHandler  *handler );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECREADBLOCK_HH_ */
