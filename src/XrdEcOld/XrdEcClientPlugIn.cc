/*
 * XrdEcFilePlugIn.cc
 *
 *  Created on: Nov 29, 2019
 *      Author: simonm
 */

#include "XrdEcClientPlugIn.hh"

namespace XrdEc
{

  FilePlugIn::FilePlugIn() : flags( XrdCl::OpenFlags::None )
  {
  }

  FilePlugIn::~FilePlugIn()
  {
  }

} /* namespace XrdEc */
