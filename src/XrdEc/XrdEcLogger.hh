/*
 * XrdEcLogger.hh
 *
 *  Created on: Nov 21, 2019
 *      Author: simonm
 */

#ifndef SRC_XRDEC_XRDECLOGGER_HH_
#define SRC_XRDEC_XRDECLOGGER_HH_

#include <fstream>
#include <string>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <memory>

namespace XrdEc
{

  class Logger
  {
      struct LogEntry
      {
          enum Status { OPEN, CLOSED };

          LogEntry() : status( OPEN )
          {
          }

          void Commit()
          {
            status = CLOSED;
          }

          std::string message;
          Status      status;
      };

    public:

      static Logger& Instance()
      {
        static Logger instance( "/tmp/xrdec.log" );
        return instance;
      }

      void Entry( std::string &&msg )
      {
        LogEntry *entry = GetEntry();
        entry->message = std::move( msg );
        Commit( entry );
      }

      void Purge()
      {
        std::unique_lock<std::mutex> lck( wrtmtx );
        fout.close();
        std::remove( path.c_str() );
        new( &fout ) std::fstream( path, std::fstream::out );
      }

    private:

      Logger( const std::string &out ) : fout( out, std::fstream::out ), worker( Work, this ), done( false ), path( out )
      {

      }

      virtual ~Logger()
      {
        {
          std::unique_lock<std::mutex> lck( mtx );
          done = true;
          cv.notify_all();
        }
        worker.join();
        fout.close();
      }

      inline LogEntry* GetEntry()
      {
        LogEntry *entry = new LogEntry();
        Enqueue( entry );
        return entry;
      }

      inline void Enqueue( LogEntry *entry )
      {
        std::unique_lock<std::mutex> lck( mtx );
        entries.push( entry );
      }

      inline void Commit( LogEntry *entry )
      {
        std::unique_lock<std::mutex> lck( mtx );
        entry->Commit();
        cv.notify_one();
      }

      static void Work( Logger *me )
      {
        while( !me->done )
        {
          std::unique_lock<std::mutex> lck( me->mtx );
          me->cv.wait( lck );

          std::queue<std::unique_ptr<LogEntry>> towrt;
          while( !me->entries.empty() && me->entries.front()->status == LogEntry::CLOSED )
          {
            towrt.emplace( me->entries.front() );
            me->entries.pop();
          }

          lck.unlock();
          me->Write( towrt );
        }
      }

      inline void Write( std::queue<std::unique_ptr<LogEntry>> &towrt )
      {
        std::unique_lock<std::mutex> lck( wrtmtx );

        while( !towrt.empty() )
        {
          std::time_t timestmp = std::time( nullptr );
          std::string timestr  = "[";
          timestr += std::asctime( std::localtime( &timestmp ) );
          timestr[timestr.size()-1] = ']';

          fout << timestr << " ";
          fout << towrt.front()->message << '\n';
          fout.flush();
          towrt.pop();
        }
      }

      std::fstream              fout;
      std::queue<LogEntry*>     entries;
      std::queue<LogEntry*>     towrt;
      std::thread               worker;
      bool                      done;
      std::mutex                mtx;
      std::condition_variable   cv;

      std::mutex                wrtmtx;
      std::string               path;
  };

} /* namespace XrdEc */

#endif /* SRC_XRDEC_XRDECLOGGER_HH_ */
