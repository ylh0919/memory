# Install script for directory: /shared/liandao/wingchun/td

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
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libctptd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libctptd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libctptd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libctptd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libctptd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libctptd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libctptd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libctptd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancetd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancetd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancetd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbinancetd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancetd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancetd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancetd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancetd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinanceftd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinanceftd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinanceftd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbinanceftd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinanceftd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinanceftd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinanceftd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinanceftd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinmextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinmextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinmextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libcoinmextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinmextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinmextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinmextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinmextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmocktd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmocktd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmocktd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libmocktd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmocktd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmocktd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmocktd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmocktd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libascendextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libascendextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libascendextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libascendextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libascendextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libascendextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libascendextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libascendextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitfinextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitfinextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitfinextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbitfinextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitfinextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitfinextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitfinextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitfinextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbittrextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbittrextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbittrextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbittrextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbittrextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbittrextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbittrextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbittrextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitmextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitmextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitmextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbitmextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitmextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitmextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitmextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitmextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhitbtctd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhitbtctd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhitbtctd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libhitbtctd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhitbtctd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhitbtctd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhitbtctd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhitbtctd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/liboceanextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanexbtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanexbtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanexbtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/liboceanexbtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanexbtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanexbtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanexbtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liboceanexbtd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libprobittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libprobittd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libprobittd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libprobittd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libprobittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libprobittd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libprobittd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libprobittd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinchecktd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinchecktd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinchecktd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libcoinchecktd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinchecktd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinchecktd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinchecktd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinchecktd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbithumbtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbithumbtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbithumbtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbithumbtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbithumbtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbithumbtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbithumbtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbithumbtd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libupbittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libupbittd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libupbittd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libupbittd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libupbittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libupbittd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libupbittd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/utils/uuid/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libupbittd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libdaybittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libdaybittd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libdaybittd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libdaybittd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libdaybittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libdaybittd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libdaybittd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libdaybittd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkrakentd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkrakentd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkrakentd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libkrakentd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkrakentd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkrakentd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkrakentd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkrakentd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitflyertd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitflyertd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitflyertd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbitflyertd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitflyertd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitflyertd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitflyertd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitflyertd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libpoloniextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libpoloniextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libpoloniextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libpoloniextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libpoloniextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libpoloniextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libpoloniextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libpoloniextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkucointd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkucointd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkucointd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libkucointd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkucointd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkucointd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkucointd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkucointd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkumextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkumextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkumextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libkumextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkumextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkumextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkumextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libkumextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libemxtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libemxtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libemxtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libemxtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libemxtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libemxtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libemxtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libemxtd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhuobitd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhuobitd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhuobitd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libhuobitd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhuobitd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhuobitd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhuobitd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhuobitd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhbdmtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhbdmtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhbdmtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libhbdmtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhbdmtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhbdmtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhbdmtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libhbdmtd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libokextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libokextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libokextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libokextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libokextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libokextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libokextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libokextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libftxtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libftxtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libftxtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libftxtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libftxtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libftxtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libftxtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libftxtd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitstamptd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitstamptd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitstamptd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbitstamptd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitstamptd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitstamptd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitstamptd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbitstamptd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinflextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinflextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinflextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libcoinflextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinflextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinflextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinflextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinflextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinfloortd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinfloortd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinfloortd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libcoinfloortd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinfloortd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinfloortd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinfloortd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libcoinfloortd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockkucointd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockkucointd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockkucointd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libmockkucointd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockkucointd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockkucointd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockkucointd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockkucointd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbitmextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbitmextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbitmextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libmockbitmextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbitmextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbitmextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbitmextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbitmextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liberisxtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liberisxtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liberisxtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/liberisxtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liberisxtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liberisxtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liberisxtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/liberisxtd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libderibittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libderibittd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libderibittd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libderibittd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libderibittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libderibittd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libderibittd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libderibittd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbequanttd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbequanttd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbequanttd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbequanttd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbequanttd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbequanttd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbequanttd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/utils/libwebsockets/lib:/shared/liandao/build/yijinjing/log:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbequanttd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockhbdmtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockhbdmtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockhbdmtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libmockhbdmtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockhbdmtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockhbdmtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockhbdmtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockhbdmtd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbinancetd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbinancetd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbinancetd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libmockbinancetd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbinancetd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbinancetd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbinancetd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockbinancetd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockcoinflextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockcoinflextd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockcoinflextd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libmockcoinflextd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockcoinflextd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockcoinflextd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockcoinflextd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockcoinflextd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockderibittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockderibittd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockderibittd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libmockderibittd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockderibittd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockderibittd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockderibittd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libmockderibittd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libqdptd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libqdptd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libqdptd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libqdptd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libqdptd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libqdptd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libqdptd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libqdptd.so")
    endif()
  endif()
endif()

if("${CMAKE_INSTALL_COMPONENT}" STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancedtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancedtd.so")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancedtd.so"
         RPATH "")
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/wingchun" TYPE SHARED_LIBRARY FILES "/shared/liandao/build/wingchun/td/libbinancedtd.so")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancedtd.so" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancedtd.so")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancedtd.so"
         OLD_RPATH "/shared/liandao/longfist/api/ctp/v6.3.15_20190220/lib:/shared/liandao/longfist/api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/lib:/shared/liandao/longfist/api/xtp/XTP_API_20171115_1.1.16.9/lib:/shared/liandao/wingchun/../yijinjing:/shared/liandao/wingchun/../monitor:/shared/liandao/build/yijinjing/journal:/shared/liandao/build/monitor/monitor_api:/opt/kungfu/toolchain/boost-1.62.0/lib:/shared/liandao/utils/cpr/lib:/shared/liandao/build/yijinjing/log:/shared/liandao/utils/libwebsockets/lib:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/wingchun/libbinancedtd.so")
    endif()
  endif()
endif()

