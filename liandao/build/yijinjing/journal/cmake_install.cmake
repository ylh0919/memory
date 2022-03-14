# Install script for directory: /shared/liandao/yijinjing/journal

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "0")
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE FILE FILES
    "/shared/liandao/yijinjing/journal/../utils/Timer.h"
    "/shared/liandao/yijinjing/journal/../utils/Hash.hpp"
    "/shared/liandao/yijinjing/journal/../utils/TypeConvert.hpp"
    "/shared/liandao/yijinjing/journal/../utils/json.hpp"
    "/shared/liandao/yijinjing/journal/../utils/PosHandler.hpp"
    "/shared/liandao/yijinjing/journal/../utils/FeeHandler.hpp"
    "/shared/liandao/yijinjing/journal/../utils/constants.h"
    "/shared/liandao/yijinjing/journal/../utils/YJJ_DECLARE.h"
    "/shared/liandao/yijinjing/journal/Frame.hpp"
    "/shared/liandao/yijinjing/journal/FrameHeader.h"
    "/shared/liandao/yijinjing/journal/Journal.h"
    "/shared/liandao/yijinjing/journal/JournalHandler.h"
    "/shared/liandao/yijinjing/journal/JournalReader.h"
    "/shared/liandao/yijinjing/journal/JournalWriter.h"
    "/shared/liandao/yijinjing/journal/Page.h"
    "/shared/liandao/yijinjing/journal/PageUtil.h"
    "/shared/liandao/yijinjing/journal/PageHeader.h"
    "/shared/liandao/yijinjing/journal/PageProvider.h"
    "/shared/liandao/yijinjing/journal/IPageProvider.h"
    "/shared/liandao/yijinjing/journal/StrategySocketHandler.h"
    "/shared/liandao/yijinjing/journal/StrategyUtil.h"
    "/shared/liandao/yijinjing/journal/IJournalVisitor.h"
    "/shared/liandao/yijinjing/journal/IStrategyUtil.h"
    "/shared/liandao/yijinjing/journal/JournalFinder.h"
    )
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  foreach(file
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/yijinjing/libjournal.so.1.1"
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/yijinjing/libjournal.so"
      )
    if(EXISTS "${file}" AND
       NOT IS_SYMLINK "${file}")
      file(RPATH_CHECK
           FILE "${file}"
           RPATH "")
    endif()
  endforeach()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/yijinjing" TYPE SHARED_LIBRARY FILES
    "/shared/liandao/build/yijinjing/journal/libjournal.so.1.1"
    "/shared/liandao/build/yijinjing/journal/libjournal.so"
    )
  foreach(file
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/yijinjing/libjournal.so.1.1"
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/yijinjing/libjournal.so"
      )
    if(EXISTS "${file}" AND
       NOT IS_SYMLINK "${file}")
      file(RPATH_CHANGE
           FILE "${file}"
           OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/opt/kungfu/toolchain/boost-1.62.0/lib:"
           NEW_RPATH "")
      if(CMAKE_INSTALL_DO_STRIP)
        execute_process(COMMAND "/usr/bin/strip" "${file}")
      endif()
    endif()
  endforeach()
endif()

