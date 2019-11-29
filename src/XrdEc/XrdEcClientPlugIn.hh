/*
 * XrdEcFilePlugIn.hh
 *
 *  Created on: Nov 29, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECCLIENTPLUGIN_HH_
#define SRC_XRDEC_XRDECCLIENTPLUGIN_HH_

#include <XrdCl/XrdClPlugInInterface.hh>

#include "XrdEc/XrdEcDataObject.hh"
#include "XrdEc/XrdEcScheduleHandler.hh"

#include <mutex>
#include <memory>
#include <exception>

namespace XrdEc
{

  class FilePlugIn : public XrdCl::FilePlugIn
  {
    public:
      FilePlugIn();

      virtual ~FilePlugIn();

      //------------------------------------------------------------------------
      //! @see XrdCl::File::Open
      //------------------------------------------------------------------------
      XrdCl::XRootDStatus Open( const std::string        &url,
                                XrdCl::OpenFlags::Flags   flags,
                                XrdCl::Access::Mode       mode,
                                XrdCl::ResponseHandler   *handler,
                                uint16_t                  timeout )
      {
        (void) mode; (void) timeout;
        std::unique_lock<std::mutex> lck( mtx );
        this->flags = flags;

        try
        {
          dataobj.reset( new DataObject( url, 0 ) );
          ScheduleHandler( handler );
        }
        catch( const std::exception &ex )
        {
          // TODO translate proper exception to XRootDStatus !!!
          return XrdCl::XRootDStatus( XrdCl::stError );
        }

        return XrdCl::XRootDStatus();
      }

      //------------------------------------------------------------------------
      //! @see XrdCl::File::Close
      //------------------------------------------------------------------------
      XrdCl::XRootDStatus Close( XrdCl::ResponseHandler *handler,
                                 uint16_t                timeout )
      {
        return Sync( handler, timeout );
      }

      //------------------------------------------------------------------------
      //! @see XrdCl::File::Read
      //------------------------------------------------------------------------
      XrdCl::XRootDStatus Read( uint64_t                offset,
                                uint32_t                size,
                                void                   *buffer,
                                XrdCl::ResponseHandler *handler,
                                uint16_t                timeout )
      {
        (void)timeout;
        std::unique_lock<std::mutex> lck( mtx );

        if( flags | XrdCl::OpenFlags::SeqIO )
          dataobj->StrmRead( offset, size, buffer, handler );
        else
          dataobj->RandomRead( offset, size, buffer, handler );

        return XrdCl::XRootDStatus();
      }

      //------------------------------------------------------------------------
      //! @see XrdCl::File::Write
      //------------------------------------------------------------------------
      XrdCl::XRootDStatus Write( uint64_t                offset,
                                 uint32_t                size,
                                 const void             *buffer,
                                 XrdCl::ResponseHandler *handler,
                                 uint16_t                timeout )
      {
        (void)timeout;
        std::unique_lock<std::mutex> lck( mtx );

        dataobj->StrmWrite( offset, size, buffer, handler );
        return XrdCl::XRootDStatus();
      }

      //------------------------------------------------------------------------
      //! @see XrdCl::File::Sync
      //------------------------------------------------------------------------
      XrdCl::XRootDStatus Sync( XrdCl::ResponseHandler *handler,
                                uint16_t                timeout )
      {
        (void)timeout;
        std::unique_lock<std::mutex> lck( mtx );

        dataobj->Flush();
        dataobj->Sync( handler );
        return XrdCl::XRootDStatus();
      }

      //------------------------------------------------------------------------
      //! @see XrdCl::File::Truncate
      //------------------------------------------------------------------------
      XrdCl::XRootDStatus Truncate( uint64_t                size,
                                    XrdCl::ResponseHandler *handler,
                                    uint16_t                timeout )
      {
        (void)timeout;
        std::unique_lock<std::mutex> lck( mtx );

        dataobj->Truncate( size, handler );
        return XrdCl::XRootDStatus();
      }

      //------------------------------------------------------------------------
      //! @see XrdCl::File::IsOpen
      //------------------------------------------------------------------------
      bool IsOpen() const
      {
        return bool( dataobj );
      }

    private:

      std::unique_ptr<DataObject> dataobj;
      XrdCl::OpenFlags::Flags     flags;
      std::mutex                  mtx;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECCLIENTPLUGIN_HH_ */
