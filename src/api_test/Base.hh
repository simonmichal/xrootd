/*
 * Base.hh
 *
 *  Created on: Jun 30, 2016
 *      Author: simonm
 */

#ifndef SRC_API_TEST_BASE_HH_
#define SRC_API_TEST_BASE_HH_

class Base
{
  public:
    Base(){}
    virtual ~Base(){}
    virtual int func() = 0;
};

typedef Base* create_t();
typedef void  destroy_t( Base* );

#endif /* SRC_API_TEST_BASE_HH_ */
