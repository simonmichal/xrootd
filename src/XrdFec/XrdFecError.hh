/*
 * XrdFecError.hh
 *
 *  Created on: Jun 22, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDFEC_XRDFECERROR_HH_
#define SRC_XRDFEC_XRDFECERROR_HH_

#include "XProtocol/XProtocol.hh"

#include <string.h>

#include <exception>

class XrdFecError : public std::exception
{
  public:
    XrdFecError( uint32_t errNo ) : mErrno( errNo ) {}

    XrdFecError( const XrdFecError &err ) : mErrno( err.mErrno ) {}

    virtual const char* what() const noexcept
    {
      return "";//strerror( mErrno ); //TODO
    }

    uint32_t Errno()
    {
      return mErrno;
    }

  private:
    uint32_t mErrno;
};

class XrdFecNotFound : public XrdFecError
{
  public:
    XrdFecNotFound() : XrdFecError( kXR_NotFound ) {}

    XrdFecNotFound( const XrdFecNotFound &err ) : XrdFecError( kXR_NotFound ) {}
};



#endif /* SRC_XRDFEC_XRDFECERROR_HH_ */
