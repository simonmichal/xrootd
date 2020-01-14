/*
 * XrdEcThreadPool.hh
 *
 *  Created on: Jan 14, 2020
 *      Author: simonm
 */

#include "XrdCl/XrdClJobManager.hh"


#ifndef SRC_XRDEC_XRDECTHREADPOOL_HH_
#define SRC_XRDEC_XRDECTHREADPOOL_HH_

namespace XrdEc
{

  class ThreadPool
  {
    private:

      // This is the type which holds sequences
      template<int ... Is> struct sequence {};

      // First define the template signature
      template <int ... Ns> struct seq_gen;

      // Recursion case
      template <int I, int ... Ns>
      struct seq_gen<I, Ns...>
      {
          using type = typename seq_gen<I - 1, I - 1, Ns...>::type;
      };

      // Recursion abort
      template <int ... Ns>
      struct seq_gen<0, Ns...>
      {
          using type = sequence<Ns...>;
      };

      template <typename FUNC, typename TUPL, int ... INDICES>
      inline static void tuple_call_impl( FUNC &f, TUPL &args, sequence<INDICES...> )
      {
          f( std::move( std::get<INDICES>( args ) )... );
      }

      template <typename FUNC, typename ... ARGs>
      inline static void tuple_call( FUNC &f, std::tuple<ARGs...> &tup )
      {
          tuple_call_impl( f, tup, typename seq_gen<sizeof...(ARGs)>::type{} );
      }

      template<typename FUNC, typename ... ARGs>
      class AnyJob : public XrdCl::Job
      {
        public:

          AnyJob( FUNC func, ARGs... args ) : func( std::move( func ) ),
                                              args( std::tuple<ARGs...>( std::move( args )... ) )
          {
          }

          void Run( void *arg )
          {
            tuple_call( func, args );
            delete this;
          }

        private:

          FUNC                func;
          std::tuple<ARGs...> args;
      };

    public:

      ~ThreadPool()
      {
        threadpool.Stop();
        threadpool.Finalize();
      }

      static ThreadPool& Instance()
      {
        static ThreadPool instance;
        return instance;
      }

      template<typename FUNC, typename ... ARGs>
      inline void Execute( FUNC func, ARGs... args )
      {
        AnyJob<FUNC, ARGs...> *job = new AnyJob<FUNC, ARGs...>( func, std::move( args )... );
        threadpool.QueueJob( job, nullptr );
      }

    private:

      ThreadPool() : threadpool( 16 )
      {
        threadpool.Initialize();
        threadpool.Start();
      }

      XrdCl::JobManager threadpool;
  };

}


#endif /* SRC_XRDEC_XRDECTHREADPOOL_HH_ */
