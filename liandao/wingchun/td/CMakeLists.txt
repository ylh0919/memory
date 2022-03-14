# Copyright [2017] [taurus.ai]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CMAKE_MINIMUM_REQUIRED(VERSION 3.7)
PROJECT(wingchuntd)

SET(COMMON_LIBRARIES journal kflog monitorapi)
SET(TD_SOURCE_FILES  ITDEngine.cpp ITDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/TDUserStruct.cpp ../base/TDUserStruct.h)

#SET(CTP_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineCTP.cpp TDEngineCTP.h)
#ADD_LIBRARY(ctptd SHARED ${CTP_SOURCE_FILES})
#TARGET_LINK_LIBRARIES(ctptd journal kflog thosttraderapi ${Boost_LIBRARIES})
#INSTALL(TARGETS ctptd DESTINATION "lib/wingchun")

#SET(XTP_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineXTP.cpp TDEngineXTP.h)
#ADD_LIBRARY(xtptd SHARED ${XTP_SOURCE_FILES})
#TARGET_LINK_LIBRARIES(xtptd journal kflog xtptraderapi ${Boost_LIBRARIES})
#INSTALL(TARGETS xtptd DESTINATION "lib/wingchun")

INCLUDE_DIRECTORIES( ${CMAKE_SOURCE_DIR}/utils/cpr/include ${CMAKE_SOURCE_DIR}/utils/rapidjson/include/ ${CMAKE_SOURCE_DIR}/utils/libwebsockets/include)

SET(CTP_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineCTP.cpp TDEngineCTP.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(ctptd SHARED ${CTP_SOURCE_FILES})
TARGET_LINK_LIBRARIES(ctptd journal kflog thosttraderapi ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS ctptd DESTINATION "lib/wingchun")

SET(BINANCE_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBinance.cpp TDEngineBinance.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(binancetd SHARED ${BINANCE_SOURCE_FILES})
TARGET_LINK_LIBRARIES(binancetd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS binancetd DESTINATION "lib/wingchun")

SET(BINANCEF_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBinanceF.cpp TDEngineBinanceF.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(binanceftd SHARED ${BINANCEF_SOURCE_FILES})
TARGET_LINK_LIBRARIES(binanceftd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS binanceftd DESTINATION "lib/wingchun")



SET(COINMEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineCoinmex.cpp TDEngineCoinmex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(coinmextd SHARED ${COINMEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coinmextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coinmextd DESTINATION "lib/wingchun")


SET(MOCK_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineMockBase.cpp TDEngineMockBase.h  TDEngineMock.cpp TDEngineMock.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mocktd SHARED ${MOCK_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mocktd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS mocktd DESTINATION "lib/wingchun")


SET(ASCENDEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineAscendEX.cpp TDEngineAscendEX.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(ascendextd SHARED ${ASCENDEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(ascendextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS ascendextd DESTINATION "lib/wingchun")


SET(BITFINEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBitfinex.cpp TDEngineBitfinex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(bitfinextd SHARED ${BITFINEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitfinextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bitfinextd DESTINATION "lib/wingchun")

SET(BITTREX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBittrex.cpp TDEngineBittrex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(bittrextd SHARED ${BITTREX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bittrextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bittrextd DESTINATION "lib/wingchun")

SET(BITMEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBitmex.cpp TDEngineBitmex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(bitmextd SHARED ${BITMEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitmextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bitmextd DESTINATION "lib/wingchun")


SET(HITBTC_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineHitBTC.cpp TDEngineHitBTC.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(hitbtctd SHARED ${HITBTC_SOURCE_FILES})
TARGET_LINK_LIBRARIES(hitbtctd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS hitbtctd DESTINATION "lib/wingchun")


SET(OCEANEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineOceanEx.cpp TDEngineOceanEx.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(oceanextd SHARED ${OCEANEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(oceanextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS oceanextd DESTINATION "lib/wingchun")

SET(OCEANEXB_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineOceanEx2.cpp TDEngineOceanEx2.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(oceanexbtd SHARED ${OCEANEXB_SOURCE_FILES})
TARGET_LINK_LIBRARIES(oceanexbtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS oceanexbtd DESTINATION "lib/wingchun")

SET(PROBIT_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineProbit.cpp TDEngineProbit.h  ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(probittd SHARED ${PROBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(probittd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS probittd DESTINATION "lib/wingchun")

SET(COINCHECK_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineCoinCheck.cpp TDEngineCoinCheck.h  ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(coinchecktd SHARED ${COINCHECK_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coinchecktd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coinchecktd DESTINATION "lib/wingchun")

SET(BITHUMB_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBithumb.cpp TDEngineBithumb.h  ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(bithumbtd SHARED ${BITHUMB_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bithumbtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS bithumbtd DESTINATION "lib/wingchun")
SET(UPBIT_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineUpbit.cpp TDEngineUpbit.h  ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(upbittd SHARED ${UPBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(upbittd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/uuid/lib/libuuid.so)
INSTALL(TARGETS upbittd DESTINATION "lib/wingchun")

SET(DAYBIT_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineDaybit.cpp TDEngineDaybit.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(daybittd SHARED ${DAYBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(daybittd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS daybittd DESTINATION "lib/wingchun")

SET(KRAKEN_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineKraken.cpp TDEngineKraken.h  ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(krakentd SHARED ${KRAKEN_SOURCE_FILES})
TARGET_LINK_LIBRARIES(krakentd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS krakentd DESTINATION "lib/wingchun")

SET(BITFLYER_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBitflyer.cpp TDEngineBitflyer.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(bitflyertd SHARED ${BITFLYER_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitflyertd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS bitflyertd DESTINATION "lib/wingchun")

SET(POLONIEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEnginePoloniex.cpp TDEnginePoloniex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(poloniextd SHARED ${POLONIEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(poloniextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS poloniextd DESTINATION "lib/wingchun")

SET(KUCOIN_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineKuCoin.cpp TDEngineKuCoin.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(kucointd SHARED ${KUCOIN_SOURCE_FILES})
TARGET_LINK_LIBRARIES(kucointd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS kucointd DESTINATION "lib/wingchun")

SET(KUMEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineKumex.cpp TDEngineKumex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(kumextd SHARED ${KUMEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(kumextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS kumextd DESTINATION "lib/wingchun")

SET(EMX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineEmx.cpp TDEngineEmx.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(emxtd SHARED ${EMX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(emxtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS emxtd DESTINATION "lib/wingchun")

SET(HUOBI_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineHuobi.cpp TDEngineHuobi.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(huobitd SHARED ${HUOBI_SOURCE_FILES})
TARGET_LINK_LIBRARIES(huobitd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS huobitd DESTINATION "lib/wingchun")

SET(HBDM_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineHbdm.cpp TDEngineHbdm.h ../base/websocket.h ../base/websocket.cpp ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(hbdmtd SHARED ${HBDM_SOURCE_FILES})
TARGET_LINK_LIBRARIES(hbdmtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS hbdmtd DESTINATION "lib/wingchun")

SET(OKEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineOkex.cpp TDEngineOkex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(okextd SHARED ${OKEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(okextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS okextd DESTINATION "lib/wingchun")

SET(FTX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineFtx.cpp TDEngineFtx.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(ftxtd SHARED ${FTX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(ftxtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS ftxtd DESTINATION "lib/wingchun")

SET(BITSTAMP_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBitstamp.cpp TDEngineBitstamp.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(bitstamptd SHARED ${BITSTAMP_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitstamptd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bitstamptd DESTINATION "lib/wingchun")

SET(COINFLEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineCoinflex.cpp TDEngineCoinflex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../../utils/libecp/include/libecp/ecp.h ../../utils/libecp/include/libecp/ecp.cpp)
ADD_LIBRARY(coinflextd SHARED ${COINFLEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coinflextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so libgmp.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coinflextd DESTINATION "lib/wingchun")

SET(COINFLOOR_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineCoinfloor.cpp TDEngineCoinfloor.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../../utils/libecp/include/libecp/ecp.h ../../utils/libecp/include/libecp/ecp.cpp)
ADD_LIBRARY(coinfloortd SHARED ${COINFLOOR_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coinfloortd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so libgmp.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coinfloortd DESTINATION "lib/wingchun")

SET(MOCKKUCOIN_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineMockBase.cpp TDEngineMockBase.h TDEngineMockKuCoin.cpp TDEngineMockKuCoin.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mockkucointd SHARED ${MOCKKUCOIN_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mockkucointd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS mockkucointd DESTINATION "lib/wingchun")

SET(MOCKBITMEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineMockBase.cpp TDEngineMockBase.h  TDEngineMockBitmex.cpp TDEngineMockBitmex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mockbitmextd SHARED ${MOCKBITMEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mockbitmextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS mockbitmextd DESTINATION "lib/wingchun")

SET(ERISX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineErisX.cpp TDEngineErisX.h  ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(erisxtd SHARED ${ERISX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(erisxtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS erisxtd DESTINATION "lib/wingchun")

SET(DERIBIT_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineDeribit.cpp TDEngineDeribit.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(deribittd SHARED ${DERIBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(deribittd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS deribittd DESTINATION "lib/wingchun")

SET(BEQUANT_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBequant.cpp TDEngineBequant.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(bequanttd SHARED ${BEQUANT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bequanttd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bequanttd DESTINATION "lib/wingchun")

SET(MOCKHBDM_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineMockBase.cpp TDEngineMockBase.h  TDEngineMockHbdm.cpp TDEngineMockHbdm.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mockhbdmtd SHARED ${MOCKHBDM_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mockhbdmtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS mockhbdmtd DESTINATION "lib/wingchun")

SET(MOCKBINANCE_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineMockBase.cpp TDEngineMockBase.h  TDEngineMockBinance.cpp TDEngineMockBinance.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mockbinancetd SHARED ${MOCKBINANCE_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mockbinancetd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS mockbinancetd DESTINATION "lib/wingchun")

SET(MOCKCOINFLEX_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineMockBase.cpp TDEngineMockBase.h  TDEngineMockCoinflex.cpp TDEngineMockCoinflex.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mockcoinflextd SHARED ${MOCKCOINFLEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mockcoinflextd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS mockcoinflextd DESTINATION "lib/wingchun")

SET(MOCKDERIBIT_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineMockBase.cpp TDEngineMockBase.h  TDEngineMockDeribit.cpp TDEngineMockDeribit.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mockderibittd SHARED ${MOCKDERIBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mockderibittd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS mockderibittd DESTINATION "lib/wingchun")

SET(QDP_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineQDP.cpp TDEngineQDP.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(qdptd SHARED ${QDP_SOURCE_FILES})
TARGET_LINK_LIBRARIES(qdptd qdptraderapi ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS qdptd DESTINATION "lib/wingchun")

SET(BINANCED_SOURCE_FILES ${TD_SOURCE_FILES} TDEngineBinanceD.cpp TDEngineBinanceD.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/InterfaceMgr.cpp ../base/InterfaceMgr.h)
ADD_LIBRARY(binancedtd SHARED ${BINANCED_SOURCE_FILES})
TARGET_LINK_LIBRARIES(binancedtd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so)
INSTALL(TARGETS binancedtd DESTINATION "lib/wingchun")