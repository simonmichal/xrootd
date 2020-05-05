/*
 * XrdEcEosAdaptor.hh
 *
 *  Created on: Oct 9, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECEOSADAPTOR_HH_
#define SRC_XRDEC_XRDECEOSADAPTOR_HH_

#include "XrdEc/XrdEcUtilities.hh"

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"

#include <exception>
#include <string>
#include <vector>
#include <sstream>
#include <tuple>

namespace XrdEc
{
  struct MgmFeedback
  {
      MgmFeedback( std::string             && objname,
                   std::string             && placementgr,
                   std::string             && signature,
                   std::vector<location_t> && urls ) :
        objname( std::move( objname ) ),
        plgrid( std::move( placementgr ) ),
        signature( std::move( signature ) ),
        plgr( std::move( urls ) )
      {
      }

      MgmFeedback( MgmFeedback && mf ) :
        objname( std::move( mf.objname ) ),
        plgrid( std::move( mf.plgrid ) ),
        signature( std::move( mf.signature ) ),
        plgr( std::move( mf.plgr ) )
      {
      }

      std::string     objname;
      std::string     plgrid;
      std::string     signature;
      placement_group plgr;
  };


  class EosAdaptor
  {
    public:
      static MgmFeedback Resolve( const std::string &path, const std::string &headnode )
      {
        using namespace XrdCl;
        // do a 'EOS plug-in' call
        FileSystem fs( headnode );
        Buffer arg; arg.FromString( path );
        Buffer *response = 0;
        XRootDStatus st = fs.Query( QueryCode::OpaqueFile, arg, response );
        if( !st.IsOK() ) throw std::exception(); // TODO
        std::string rsp = response->ToString();
        size_t pos = rsp.find( '?' );
        // parse the object name
        std::string objname = rsp.substr( 0, pos );
        // parse the cgi
        URL fake( "fake://fake:1111//" + rsp );
        URL::ParamsMap params = fake.GetParams();
        // the placement group
        std::string placementgr = params["ost.group"];
        // the signature
        std::string signature   = params["ost.sig"];
        // resolve the placement group
        std::string queryurls = headnode + "/proc/user/?mgm.pcmd=ost.pg&mgm.ost.pg=" + placementgr;
        // we will get the urls by reading a proc file
        File proc_user( false );
        st = proc_user.Open( queryurls, OpenFlags::Read );
        if( !st.IsOK() ) throw std::exception(); // TODO
        StatInfo *info = 0;
        st = proc_user.Stat( false, info );
        if( !st.IsOK() ) throw std::exception(); // TODO
        uint64_t urlssize = info->GetSize();
        delete info;
        char *buffer = new char[urlssize];
        uint32_t bytesRead = 0;
        st = proc_user.Read( 0, urlssize, buffer, bytesRead );
        if( !st.IsOK() ) throw std::exception(); // TODO
        st = proc_user.Close();
        if( !st.IsOK() ) throw std::exception(); // TODO

        std::string locationsstr( buffer, bytesRead );
        delete[] buffer;

        std::stringstream ss;
        ss << locationsstr;
        std::string entry;
        placement_group plgr;

//        while( std::getline( ss, entry, '\n' ) )
//        {
//          if( !entry.empty() )
//          {
//            size_t pos = entry.find( ' ' );
//            std::string url    = entry.substr( 0, pos );
//            std::string status = entry.substr( pos );
//            locations.emplace_back( url, status );
//          }
//        }

        return MgmFeedback( std::move( objname ),
                            std::move( placementgr ),
                            std::move( signature ),
                            std::move( plgr ) );
      }

      static MgmFeedback ResolveDummy( const std::string &path, const std::string &headnode )
      {
        // <inode-hex>-<EC-alg>-<EC-params>-<blocksize-MB>.<generation>.<block-nr> // block nr to be appended once it is known!
        std::string objname     = "0000000000000001-isa-12:4-4.0"; // example object name
        std::string placementgr = "abcdfg"; // some string to identify the placement group
        std::string signature   = "abcdef"; // a signature

        placement_group plgr;
        for( size_t i = 0; i < 16; ++i )
        {
          std::string url = "file://localhost/data/dir" + std::to_string( i );
          plgr.emplace_back( url, rw );
        }

        return MgmFeedback( std::move( objname ),
                            std::move( placementgr ),
                            std::move( signature ),
                            std::move( plgr ) );
      }
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECEOSADAPTOR_HH_ */
