/*
 * xrdec2_test.cc
 *
 *  Created on: Jan 27, 2020
 *      Author: simonm
 */




#include "XrdEc/XrdEcDataObject.hh"
#include "XrdCl/XrdClMessageUtils.hh"

#include <iostream>
#include <fstream>

int main( int argc, char **argv )
{
  std::cout << "There we go!" << std::endl;

  size_t _4KB = 4 * 1024;
  XrdEc::ObjCfg objcfg( "0000000000000001", "0001", 8, 4, _4KB );
  std::vector<std::string> plgr{
                                 "file://localhost/data/srv0/",
                                 "file://localhost/data/srv1/",
                                 "file://localhost/data/srv2/",
                                 "file://localhost/data/srv3/",
                                 "file://localhost/data/srv4/",
                                 "file://localhost/data/srv5/",
                                 "file://localhost/data/srv6/",
                                 "file://localhost/data/srv7/",
                                 "file://localhost/data/srv8/",
                                 "file://localhost/data/srv9/",
                                 "file://localhost/data/srv10/",
                                 "file://localhost/data/srv11/",
                               };
  objcfg.plgr = std::move( plgr );

  std::cout << "open" << std::endl;
  XrdCl::SyncResponseHandler h1;
  XrdEc::DataObject obj;
  obj.Open( objcfg, XrdEc::StrmWrtMode, &h1 );
  h1.WaitForResponse();
  XrdCl::XRootDStatus *s1 = h1.GetStatus();
  std::cout << s1->ToString() << std::endl;
  delete s1;

  size_t size  = 8 * _4KB;
  size_t cnt = 3;
  char *buffer[cnt];
  for( size_t i = 0; i < cnt; ++i )
  {
    buffer[i] = new char[size];
    memset( buffer[i], 'A' + i, size );
  }

  std::cout << "write" << std::endl;
  uint64_t offset = 0;
  for( size_t i = 0; i < cnt; ++i )
  {
    XrdCl::SyncResponseHandler h2;
    if( i < 2 )
      obj.Write( offset, size, buffer[i], &h2 );
    else
      obj.Write( offset, 4 * _4KB + 2048, buffer[i], &h2 );
    h2.WaitForResponse();
    XrdCl::XRootDStatus *s2 = h2.GetStatus();
    std::cout << s2->ToString() << std::endl;
    delete s2;
    
    offset += size;
  }

  std::cout << "close" << std::endl;
  XrdCl::SyncResponseHandler h3;
  obj.Close( &h3 );
  h3.WaitForResponse();
  XrdCl::XRootDStatus *s3 = h3.GetStatus();
  std::cout << s3->ToString() << std::endl;
  delete s3;

//  std::string path = "bible.txt";
//  size_t biblesize = 4047392;
//  std::unique_ptr<char[]> bigbuff( new char[biblesize + 1024] ); // big buffer than can accommodate the whole bible plus bit more
//  std::ifstream fs;
//  fs.open( path, std::fstream::in );
//  fs.read( bigbuff.get(), biblesize );
//  fs.close();
//  bigbuff[biblesize] = 0;
//
//  {
//    XrdEc::DataObject obj;
//    XrdCl::SyncResponseHandler handler;
//    obj.Open( objcfg, XrdEc::DataObject::WriteMode, &handler );
//    handler.WaitForResponse();
//    XrdCl::XRootDStatus *status = handler.GetStatus();
//    std::cout << "Open (for write) : " << status->ToString() << std::endl;
//    delete status;
//
//    uint32_t wrtsize = biblesize;
//    uint64_t offset  = 0;
//    char *wrtbuff = bigbuff.get();
//    int i = 0;
//
//    while( wrtsize > 0 && i < 64 )
//    {
//      ++i;
//
//      uint32_t length  = objcfg.datasize;
//      if( length > wrtsize ) length = wrtsize;
//      XrdCl::SyncResponseHandler handler2;
//      obj.Write( offset, length, wrtbuff, &handler2 );
//      handler2.WaitForResponse();
//      auto status2 = handler2.GetStatus();
//      std::cout << "Write (" << i << ") : " << status2->ToString() << std::endl;
//      delete status2;
//      wrtsize -= length;
//      offset  += length;
//      wrtbuff += length;
//    }
//
//    XrdCl::SyncResponseHandler handler3;
//    obj.Close( &handler3 );
//    handler3.WaitForResponse();
//    auto status3 = handler3.GetStatus();
//    std::cout << "Close (after write) : " << status3->ToString() << std::endl;
//    delete status3;
//  }
//
//  std::cout << std::endl;
//
//  {
//    XrdEc::ObjCfg objcfg( "0000000000000001-isa-12:4-4" );
//    XrdEc::DataObject obj;
//    XrdCl::SyncResponseHandler handler;
//    obj.Open( objcfg, XrdEc::DataObject::StrmRdMode, &handler );
//    handler.WaitForResponse();
//    XrdCl::XRootDStatus *status = handler.GetStatus();
//    std::cout << "Open (for read) : " << status->ToString() << std::endl;
//    delete status;
//
//
//    uint32_t rdsize = biblesize;
//    uint64_t offset = 0;
//    uint32_t length = objcfg.datasize;
//    std::unique_ptr<char[]> rdbuff( new char[length] );
//    int i = 0;
//
//    while( rdsize > 0 && i < 64 )
//    {
//      ++i;
//
//      XrdCl::SyncResponseHandler handler2;
//      obj.Read( offset, length, rdbuff.get(), &handler2 );
//      handler2.WaitForResponse();
//      auto status2 = handler2.GetStatus();
//      std::cout << "Read (" << i << ") : " << status2->ToString() << std::endl;
//      delete status2;
//
//      if( strncmp( rdbuff.get(), bigbuff.get(), length ) )
//      {
//        std::cout << "Data don't match the reference:" << std::endl;
//        std::cout << std::string( rdbuff.get(), length ) << std::endl;
//        return 1;
//      }
//
//      rdsize -= length;
//      offset += length;
//    }
//
//
//    XrdCl::SyncResponseHandler handler3;
//    obj.Close( &handler3 );
//    handler3.WaitForResponse();
//    auto status3 = handler3.GetStatus();
//    std::cout << "Close (after read) : " << status3->ToString() << std::endl;
//    delete status3;
//  }

  std::cout << "The End." << std::endl;

  return 0;
}
