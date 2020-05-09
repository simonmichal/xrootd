/*
 * PlacementHandler.cpp
 *
 *  Created on: 8 May 2020
 *      Author: simonm
 */

#include "PlacementHandler.hh"

namespace
{
  std::vector<std::string> st001_hba0 = {
    "root://st-096-100gb001.cern.ch:2094//hba0data/data0/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data1/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data2/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data3/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data4/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data5/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data6/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data7/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data8/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data9/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data10/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data11/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data12/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data13/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data14/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data15/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data16/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data17/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data18/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data19/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data20/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data21/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data22/test/",
    "root://st-096-100gb001.cern.ch:2094//hba0data/data23/test/",
    ""
  };

  std::vector<std::string> st001_hba1 = {
    "root://st-096-100gb001.cern.ch:2095//hba1data/data0/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data1/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data2/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data3/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data4/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data5/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data6/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data7/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data8/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data9/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data10/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data11/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data12/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data13/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data14/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data15/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data16/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data17/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data18/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data19/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data20/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data21/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data22/test/",
    "root://st-096-100gb001.cern.ch:2095//hba1data/data23/test/",
    ""
  };

  std::vector<std::string> st001_hba2 = {
    "root://st-096-100gb001.cern.ch:2096//hba2data/data0/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data1/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data2/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data3/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data4/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data5/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data6/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data7/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data8/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data9/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data10/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data11/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data12/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data13/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data14/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data15/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data16/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data17/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data18/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data19/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data20/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data21/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data22/test/",
    "root://st-096-100gb001.cern.ch:2096//hba2data/data23/test/",
    ""
  };

  std::vector<std::string> st001_hba3 = {
    "root://st-096-100gb001.cern.ch:2097//hba3data/data0/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data1/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data2/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data3/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data4/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data5/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data6/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data7/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data8/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data9/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data10/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data11/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data12/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data13/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data14/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data15/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data16/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data17/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data18/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data19/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data20/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data21/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data22/test/",
    "root://st-096-100gb001.cern.ch:2097//hba3data/data23/test/",
    ""
  };

  std::vector<std::string> st002_hba0 = {
    "root://st-096-100gb002.cern.ch:2094//hba0data/data0/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data1/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data2/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data3/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data4/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data5/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data6/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data7/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data8/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data9/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data10/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data11/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data12/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data13/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data14/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data15/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data16/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data17/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data18/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data19/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data20/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data21/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data22/test/",
    "root://st-096-100gb002.cern.ch:2094//hba0data/data23/test/",
    ""
  };

  std::vector<std::string> st002_hba1 = {
    "root://st-096-100gb002.cern.ch:2095//hba1data/data0/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data1/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data2/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data3/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data4/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data5/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data6/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data7/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data8/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data9/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data10/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data11/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data12/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data13/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data14/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data15/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data16/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data17/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data18/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data19/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data20/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data21/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data22/test/",
    "root://st-096-100gb002.cern.ch:2095//hba1data/data23/test/",
    ""
  };

  std::vector<std::string> st002_hba2 = {
    "root://st-096-100gb002.cern.ch:2096//hba2data/data0/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data1/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data2/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data3/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data4/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data5/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data6/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data7/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data8/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data9/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data10/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data11/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data12/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data13/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data14/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data15/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data16/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data17/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data18/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data19/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data20/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data21/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data22/test/",
    "root://st-096-100gb002.cern.ch:2096//hba2data/data23/test/",
    ""
  };

  std::vector<std::string> st002_hba3 = {
    "root://st-096-100gb002.cern.ch:2097//hba3data/data0/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data1/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data2/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data3/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data4/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data5/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data6/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data7/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data8/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data9/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data10/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data11/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data12/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data13/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data14/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data15/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data16/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data17/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data18/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data19/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data20/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data21/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data22/test/",
    "root://st-096-100gb002.cern.ch:2097//hba3data/data23/test/",
    ""
  };

  std::vector<std::string> st003_hba0 = {
    "root://st-096-100gb003.cern.ch:2094//hba0data/data0/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data1/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data2/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data3/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data4/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data5/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data6/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data7/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data8/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data9/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data10/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data11/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data12/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data13/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data14/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data15/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data16/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data17/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data18/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data19/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data20/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data21/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data22/test/",
    "root://st-096-100gb003.cern.ch:2094//hba0data/data23/test/",
    ""
  };

  std::vector<std::string> st003_hba1 = {
    "root://st-096-100gb003.cern.ch:2095//hba1data/data0/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data1/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data2/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data3/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data4/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data5/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data6/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data7/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data8/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data9/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data10/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data11/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data12/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data13/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data14/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data15/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data16/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data17/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data18/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data19/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data20/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data21/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data22/test/",
    "root://st-096-100gb003.cern.ch:2095//hba1data/data23/test/",
    ""
  };

  std::vector<std::string> st003_hba2 = {
    "root://st-096-100gb003.cern.ch:2096//hba2data/data0/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data1/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data2/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data3/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data4/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data5/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data6/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data7/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data8/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data9/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data10/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data11/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data12/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data13/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data14/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data15/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data16/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data17/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data18/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data19/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data20/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data21/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data22/test/",
    "root://st-096-100gb003.cern.ch:2096//hba2data/data23/test/",
    ""
  };

  std::vector<std::string> st003_hba3 = {
    "root://st-096-100gb003.cern.ch:2097//hba3data/data0/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data1/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data2/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data3/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data4/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data5/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data6/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data7/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data8/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data9/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data10/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data11/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data12/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data13/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data14/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data15/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data16/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data17/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data18/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data19/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data20/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data21/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data22/test/",
    "root://st-096-100gb003.cern.ch:2097//hba3data/data23/test/",
    ""
  };

  std::vector<std::string> st004_hba0 = {
    "root://st-096-100gb004.cern.ch:2094//hba0data/data0/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data1/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data2/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data3/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data4/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data5/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data6/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data7/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data8/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data9/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data10/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data11/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data12/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data13/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data14/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data15/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data16/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data17/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data18/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data19/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data20/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data21/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data22/test/",
    "root://st-096-100gb004.cern.ch:2094//hba0data/data23/test/",
    ""
  };

  std::vector<std::string> st004_hba1 = {
    "root://st-096-100gb004.cern.ch:2095//hba1data/data0/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data1/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data2/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data3/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data4/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data5/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data6/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data7/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data8/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data9/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data10/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data11/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data12/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data13/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data14/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data15/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data16/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data17/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data18/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data19/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data20/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data21/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data22/test/",
    "root://st-096-100gb004.cern.ch:2095//hba1data/data23/test/",
    ""
  };

  std::vector<std::string> st004_hba2 = {
    "root://st-096-100gb004.cern.ch:2096//hba2data/data0/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data1/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data2/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data3/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data4/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data5/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data6/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data7/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data8/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data9/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data10/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data11/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data12/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data13/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data14/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data15/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data16/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data17/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data18/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data19/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data20/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data21/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data22/test/",
    "root://st-096-100gb004.cern.ch:2096//hba2data/data23/test/",
    ""
  };

  std::vector<std::string> st004_hba3 = {
    "root://st-096-100gb004.cern.ch:2097//hba3data/data0/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data1/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data2/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data3/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data4/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data5/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data6/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data7/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data8/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data9/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data10/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data11/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data12/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data13/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data14/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data15/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data16/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data17/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data18/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data19/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data20/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data21/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data22/test/",
    "root://st-096-100gb004.cern.ch:2097//hba3data/data23/test/",
    ""
  };

  std::vector<std::string> st005_hba0 = {
    "root://st-096-100gb005.cern.ch:2094//hba0data/data0/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data1/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data2/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data3/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data4/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data5/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data6/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data7/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data8/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data9/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data10/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data11/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data12/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data13/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data14/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data15/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data16/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data17/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data18/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data19/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data20/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data21/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data22/test/",
    "root://st-096-100gb005.cern.ch:2094//hba0data/data23/test/",
    ""
  };

  std::vector<std::string> st005_hba1 = {
    "root://st-096-100gb005.cern.ch:2095//hba1data/data0/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data1/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data2/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data3/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data4/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data5/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data6/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data7/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data8/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data9/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data10/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data11/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data12/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data13/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data14/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data15/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data16/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data17/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data18/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data19/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data20/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data21/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data22/test/",
    "root://st-096-100gb005.cern.ch:2095//hba1data/data23/test/",
    ""
  };

  std::vector<std::string> st005_hba2 = {
    "root://st-096-100gb005.cern.ch:2096//hba2data/data0/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data1/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data2/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data3/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data4/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data5/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data6/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data7/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data8/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data9/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data10/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data11/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data12/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data13/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data14/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data15/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data16/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data17/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data18/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data19/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data20/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data21/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data22/test/",
    "root://st-096-100gb005.cern.ch:2096//hba2data/data23/test/",
    ""
  };

  std::vector<std::string> st005_hba3 = {
    "root://st-096-100gb005.cern.ch:2097//hba3data/data0/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data1/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data2/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data3/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data4/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data5/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data6/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data7/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data8/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data9/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data10/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data11/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data12/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data13/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data14/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data15/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data16/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data17/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data18/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data19/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data20/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data21/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data22/test/",
    "root://st-096-100gb005.cern.ch:2097//hba3data/data23/test/",
    ""
  };

  std::vector<std::string> st006_hba0 = {
    "root://st-096-100gb006.cern.ch:2094//hba0data/data0/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data1/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data2/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data3/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data4/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data5/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data6/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data7/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data8/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data9/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data10/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data11/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data12/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data13/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data14/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data15/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data16/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data17/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data18/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data19/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data20/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data21/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data22/test/",
    "root://st-096-100gb006.cern.ch:2094//hba0data/data23/test/",
    ""
  };

  std::vector<std::string> st006_hba1 = {
    "root://st-096-100gb006.cern.ch:2095//hba1data/data0/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data1/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data2/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data3/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data4/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data5/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data6/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data7/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data8/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data9/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data10/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data11/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data12/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data13/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data14/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data15/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data16/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data17/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data18/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data19/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data20/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data21/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data22/test/",
    "root://st-096-100gb006.cern.ch:2095//hba1data/data23/test/",
    ""
  };

  std::vector<std::string> st006_hba2 = {
    "root://st-096-100gb006.cern.ch:2096//hba2data/data0/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data1/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data2/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data3/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data4/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data5/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data6/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data7/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data8/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data9/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data10/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data11/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data12/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data13/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data14/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data15/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data16/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data17/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data18/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data19/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data20/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data21/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data22/test/",
    "root://st-096-100gb006.cern.ch:2096//hba2data/data23/test/",
    ""
  };

  std::vector<std::string> st006_hba3 = {
    "root://st-096-100gb006.cern.ch:2097//hba3data/data0/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data1/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data2/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data3/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data4/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data5/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data6/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data7/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data8/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data9/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data10/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data11/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data12/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data13/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data14/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data15/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data16/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data17/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data18/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data19/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data20/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data21/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data22/test/",
    "root://st-096-100gb006.cern.ch:2097//hba3data/data23/test/",
    ""
  };
}


std::queue<PlacementHandler::placement_t> PlacementHandler::AllocDisks( uint8_t srvnb )
{
  std::queue<placement_t> ret;

  switch( srvnb )
  {
    case 0:
    {
      for( size_t i = 0; i < 12; ++i )
      {
        placement_t p = { st001_hba0[i],
                          st002_hba0[i],
                          st003_hba0[i],
                          st004_hba0[i],
                          st005_hba0[i],
                          st006_hba0[i],
                          st001_hba1[i],
                          st002_hba1[i],
                          st003_hba1[i],
                          st004_hba1[i],
                          st005_hba1[i],
                          st006_hba1[i],
                        };
        ret.push( std::move( p ) );
      }
      break;
    }

    case 1:
    {
      for( size_t i = 12; i < 24; ++i )
      {
        placement_t p = { st001_hba0[i],
                          st002_hba0[i],
                          st003_hba0[i],
                          st004_hba0[i],
                          st005_hba0[i],
                          st006_hba0[i],
                          st001_hba1[i],
                          st002_hba1[i],
                          st003_hba1[i],
                          st004_hba1[i],
                          st005_hba1[i],
                          st006_hba1[i],
                        };
        ret.push( std::move( p ) );
      }
      break;
    }

    case 2:
    {
      for( size_t i = 0; i < 12; ++i )
      {
        placement_t p = { st001_hba2[i],
                          st002_hba2[i],
                          st003_hba2[i],
                          st004_hba2[i],
                          st005_hba2[i],
                          st006_hba2[i],
                          st001_hba3[i],
                          st002_hba3[i],
                          st003_hba3[i],
                          st004_hba3[i],
                          st005_hba3[i],
                          st006_hba3[i],
                        };
        ret.push( std::move( p ) );
      }
      break;
    }

    case 3:
    {
      for( size_t i = 12; i < 24; ++i )
      {
        placement_t p = { st001_hba2[i],
                          st002_hba2[i],
                          st003_hba2[i],
                          st004_hba2[i],
                          st005_hba2[i],
                          st006_hba2[i],
                          st001_hba3[i],
                          st002_hba3[i],
                          st003_hba3[i],
                          st004_hba3[i],
                          st005_hba3[i],
                          st006_hba3[i],
                        };
        ret.push( std::move( p ) );
      }
      break;
    }
  }

  return std::move( ret );
}

