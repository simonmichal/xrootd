/*
 * Derived.hh
 *
 *  Created on: Jun 30, 2016
 *      Author: simonm
 */

#ifndef SRC_API_TEST_DERIVED_HH_
#define SRC_API_TEST_DERIVED_HH_

#include "Base.hh"

class Derived : public Base
{
  public:
    Derived();
    virtual ~Derived();
    int func();
};

#endif /* SRC_API_TEST_DERIVED_HH_ */
