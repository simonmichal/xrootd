/*
 * XrdEcUtilities.hh
 *
 *  Created on: Jan 9, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECUTILITIES_HH_
#define SRC_XRDEC_XRDECUTILITIES_HH_

#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClFileSystem.hh"

#include <exception>
#include <memory>
#include <random>

namespace XrdEc
{
  //----------------------------------------------------------------------------
  //! a placement type (list of hosts)
  //----------------------------------------------------------------------------
  typedef std::vector<std::string>  placement_t;

  typedef std::vector<std::string> placement_group;

  //----------------------------------------------------------------------------
  //! a buffer type
  //----------------------------------------------------------------------------
  typedef std::vector<char>  buffer_t;

  //----------------------------------------------------------------------------
  //! Generic I/O exception, wraps up XrdCl::XRootDStatus (@see XRootDStatus)
  //----------------------------------------------------------------------------
  class IOError : public std::exception
  {
    public:

      //------------------------------------------------------------------------
      //! Constructor
      //!
      //! @param st : status
      //------------------------------------------------------------------------
      IOError( const XrdCl::XRootDStatus &st ) noexcept : st( st ), msg( st.ToString() )
      {
      }

      //------------------------------------------------------------------------
      //! Copy constructor
      //------------------------------------------------------------------------
      IOError( const IOError &err ) noexcept : st( err.st ), msg( err.st.ToString() )
      {
      }

      //------------------------------------------------------------------------
      //! Assigment operator
      //------------------------------------------------------------------------
      IOError& operator=( const IOError &err ) noexcept
      {
        st = err.st;
        msg = err.st.ToString();
        return *this;
      }

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~IOError()
      {

      }

      //------------------------------------------------------------------------
      //! overloaded @see std::exception
      //------------------------------------------------------------------------
      virtual const char* what() const noexcept
      {
        return msg.c_str();
      }

      //------------------------------------------------------------------------
      //! @return : the status
      //------------------------------------------------------------------------
      const XrdCl::XRootDStatus& Status() const
      {
        return st;
      }

      enum
      {
        ioTooManyErrors
      };

    private:

      //------------------------------------------------------------------------
      //! The status object
      //------------------------------------------------------------------------
      XrdCl::XRootDStatus st;

      //------------------------------------------------------------------------
      //! The error message
      //------------------------------------------------------------------------
      std::string msg;
  };


  //----------------------------------------------------------------------------
  //! Utility class for handling chunk buffers
  //!
  //! We are always reading the whole data chunk because we want to compare the
  //! checksum. If user buffer is big enough to accommodate a whole chunk it will
  //! be used, otherwise ChunkBuffer will allocate memory for the data chunk and
  //! upon destruction will copy the respective part of data into the user buffer.
  //----------------------------------------------------------------------------
  class ChunkBuffer
  {
    public:

      //------------------------------------------------------------------------
      //! Constructor.
      //!
      //! @param offset : offset in the chunk
      //! @param size   : size of the read from the chunk
      //! @param dst    : the user buffer for the read
      //------------------------------------------------------------------------
      ChunkBuffer( uint64_t offset, uint64_t size, char* dst );

      //------------------------------------------------------------------------
      //! Destructor.
      //!
      //! Upon destruction data will be copied into user buffer if necessary.
      //------------------------------------------------------------------------
      ~ChunkBuffer()
      {
        // do we have both a private buffer and a user buffer?
        if( buffer && userbuff )
          memcpy( userbuff, buffer.get() + offset, size );
      }

      //------------------------------------------------------------------------
      //! @return : pointer to the buffer
      //------------------------------------------------------------------------
      void* Get()
      {
        if( buffer ) return buffer.get();
        return userbuff;
      }

      //------------------------------------------------------------------------
      //! Marks the data in the buffer as invalid
      //------------------------------------------------------------------------
      void Invalidate()
      {
        valid = false;
      }

      //------------------------------------------------------------------------
      //! @return : true if data in the buffer are valid, false otherwise
      //------------------------------------------------------------------------
      bool IsValid() const
      {
        return valid;
      }

    private:

      //------------------------------------------------------------------------
      //! private buffer
      //------------------------------------------------------------------------
      std::unique_ptr<char[]> buffer;

      //------------------------------------------------------------------------
      //! Flag indicating whether the data in the buffer are valid
      //------------------------------------------------------------------------
      bool  valid;

      //------------------------------------------------------------------------
      //! Chunk relative offset of the read
      //------------------------------------------------------------------------
      uint64_t  offset;

      //------------------------------------------------------------------------
      //! Chunk relative size of the read
      //------------------------------------------------------------------------
      uint64_t  size;

      //------------------------------------------------------------------------
      //! User buffer fot the data
      //------------------------------------------------------------------------
      char     *userbuff;
  };

  //----------------------------------------------------------------------------
  //! Alias for a shared pointer to ChunkBuffer (@see ChunkBuffer)
  //----------------------------------------------------------------------------
  typedef std::shared_ptr<ChunkBuffer> chbuff;

  //----------------------------------------------------------------------------
  //! Utility function for creating chunk buffers
  //----------------------------------------------------------------------------
  inline chbuff make_chbuff( uint64_t offset = 0, uint64_t size = 0, char* dst = nullptr )
  {
    return std::make_shared<ChunkBuffer>( offset, size, dst );
  }


  //----------------------------------------------------------------------------
  //! Creates a sufix for a data chunk file
  //!
  //! @param blkid   : block ID
  //! @param chunkid : chunk ID
  //!
  //! @return        : sufix for a data chunk file
  //----------------------------------------------------------------------------
  inline std::string Sufix( uint64_t blkid, uint64_t chunkid )
  {
    return '.' + std::to_string( blkid ) + '.' + std::to_string( chunkid );
  }


  //----------------------------------------------------------------------------
  //! Calculates checksum of given type from given buffer
  //!
  //! @param type   : checksum type (e.g. zcrc32)
  //! @param buffer : buffer with data for checksumming
  //! @param size   : size of the buffer
  //!
  //! @return       : the checksum in the format 'type:checksum
  //----------------------------------------------------------------------------
  std::string Checksum( const std::string &type, void *buffer, uint32_t size );

  //------------------------------------------------------------------------
  //! Find a new location (host) for given chunk. TODO (update)
  //!
  //! @param chunkid   : ID of the chunk to be relocated
  //! @param relocate  : true if the chunk should be relocated even if
  //                     a placement for it already exists, false otherwise
  //------------------------------------------------------------------------
  XrdCl::OpenFlags::Flags Place( uint8_t                      chunkid,
                                 placement_t                 &placement,
                                 std::default_random_engine  &generator,
                                 const placement_group       &plgr,
                                 bool                         relocate );

}

#endif /* SRC_XRDEC_XRDECUTILITIES_HH_ */
