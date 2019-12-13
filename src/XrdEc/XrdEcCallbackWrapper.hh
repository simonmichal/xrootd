/*
 * XrdEcCallbackWrapper.hh
 *
 *  Created on: Dec 11, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECCALLBACKWRAPPER_HH_
#define SRC_XRDEC_XRDECCALLBACKWRAPPER_HH_

#include "XrdCl/XrdClXRootDResponses.hh"

#include <mutex>
#include <memory>
#include <functional>

namespace XrdEc
{

  class CallbackWrapper
  {
    public:

      CallbackWrapper( XrdCl::ResponseHandler *handler ) : handler( handler )
      {
      }

      inline operator std::mutex&()
      {
        return mtx;
      }

      inline bool Valid()
      {
        return handler;
      }

      inline XrdCl::ResponseHandler* Get()
      {
        return handler;
      }

      inline XrdCl::ResponseHandler* Release()
      {
        XrdCl::ResponseHandler *ret = handler;
        handler = nullptr;
        return ret;
      }

    private:

      XrdCl::ResponseHandler *handler;
      std::mutex              mtx;

  };

// TODO : refactor with following concepts: ...

//  class usr_handler
//  {
//    public:
//
//      usr_handler( XrdCl::ResponseHandler *handler ) : handler( handler )
//      {
//      }
//
//      inline operator std::mutex&()
//      {
//        return mtx;
//      }
//
//      inline bool valid()
//      {
//        return handler;
//      }
//
//      inline XrdCl::ResponseHandler* get()
//      {
//        return handler;
//      }
//
//      inline XrdCl::ResponseHandler* release()
//      {
//        XrdCl::ResponseHandler *ret = handler;
//        handler = nullptr;
//        return ret;
//      }
//
//    private:
//
//      XrdCl::ResponseHandler *handler;
//      std::mutex              mtx;
//  };
//
//  typedef std::shared_ptr<std::function<void(std::shared_ptr<usr_handler>&, XrdCl::XRootDStatus*, XrdCl::AnyObject*)>> shared_callback;
//
//  typedef std::shared_ptr<usr_handler> shared_handler;
}


#endif /* SRC_XRDEC_XRDECCALLBACKWRAPPER_HH_ */
