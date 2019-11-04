/*
 * XrdEcStrmWriter.hh
 *
 *  Created on: Oct 14, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECWRITEBLOCK_HH_
#define SRC_XRDEC_XRDECWRITEBLOCK_HH_

#include "XrdEc/XrdEcUtilities.hh"
#include <string>


namespace XrdEc
{
  class WrtBuff;

  void WriteBlock( const std::string      &obj,
                   const std::string      &sign,
                   const placement_group  &plgr,
                   uint64_t                fnlsize,
                   WrtBuff               &&wrtbuff,
                   XrdCl::ResponseHandler *handler );

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECWRITEBLOCK_HH_ */
