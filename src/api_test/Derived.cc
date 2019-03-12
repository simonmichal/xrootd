/*
 * Derived.cc
 *
 *  Created on: Jun 30, 2016
 *      Author: simonm
 */

#include "Derived.hh"

#include <iostream>

Derived::Derived()
{
  std::cout << "Constructing Derived." << '\n';
}

Derived::~Derived()
{
  std::cout << "Destroying Derived." << '\n';
}

int Derived::func()
{
  std::cout << "Invoking func." << '\n';
  return 5;
}

extern "C" Base* create() {
    return new Derived;
}

extern "C" void destroy( Derived* d ) {
    delete d;
}
