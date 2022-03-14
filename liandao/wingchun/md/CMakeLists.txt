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
PROJECT(wingchunmd)

#SET(XTP_SOURCE_FILES MDEngineXTP.cpp MDEngineXTP.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h)
#ADD_LIBRARY(xtpmd SHARED ${XTP_SOURCE_FILES})
#TARGET_LINK_LIBRARIES(xtpmd journal kflog xtpquoteapi ${Boost_LIBRARIES})
#INSTALL(TARGETS xtpmd DESTINATION "lib/wingchun")

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/utils/poco)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/utils/cpr/include ${CMAKE_SOURCE_DIR}/utils/rapidjson/include/ ${CMAKE_SOURCE_DIR}/utils/libwebsockets/include)
SET(COMMON_LIBRARIES journal kflog monitorapi)

#SET(CTP_SOURCE_FILES MDEngineCTP.cpp MDEngineCTP.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h)
#ADD_LIBRARY(ctpmd SHARED ${CTP_SOURCE_FILES})
#TARGET_LINK_LIBRARIES(ctpmd journal kflog thostmduserapi ${COMMON_LIBRARIES} ${Boost_LIBRARIES})
#INSTALL(TARGETS ctpmd DESTINATION "lib/wingchun")

SET(CTP_SOURCE_FILES MDEngineCTP.cpp MDEngineCTP.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(ctpmd SHARED ${CTP_SOURCE_FILES})
TARGET_LINK_LIBRARIES(ctpmd thostmduserapi journal kflog ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS ctpmd DESTINATION "lib/wingchun")

SET(BINANCE_SOURCE_FILES MDEngineBinance.cpp MDEngineBinance.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(binancemd SHARED ${BINANCE_SOURCE_FILES})
TARGET_LINK_LIBRARIES(binancemd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS binancemd DESTINATION "lib/wingchun")

SET(OKEX_SOURCE_FILES MDEngineOkex.cpp MDEngineOkex.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/websocket.h ../base/websocket.cpp PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(okexmd SHARED ${OKEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(okexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS okexmd DESTINATION "lib/wingchun")

SET(COINMEX_SOURCE_FILES MDEngineCoinmex.cpp MDEngineCoinmex.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(coinmexmd SHARED ${COINMEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coinmexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coinmexmd DESTINATION "lib/wingchun")

SET(MOCK_SOURCE_FILES MDEngineMock.cpp MDEngineMock.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h)
ADD_LIBRARY(mockmd SHARED ${MOCK_SOURCE_FILES})
TARGET_LINK_LIBRARIES(mockmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS mockmd DESTINATION "lib/wingchun")

SET(ASCENDEX_SOURCE_FILES MDEngineAscendEX.cpp MDEngineAscendEX.h  IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(ascendexmd SHARED ${ASCENDEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(ascendexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES}  ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS ascendexmd DESTINATION "lib/wingchun")

SET(BITFINEX_SOURCE_FILES MDEngineBitfinex.cpp MDEngineBitfinex.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/websocket.h ../base/websocket.cpp PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(bitfinexmd SHARED ${BITFINEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitfinexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bitfinexmd DESTINATION "lib/wingchun")

SET(BITMEX_SOURCE_FILES MDEngineBitmex.cpp MDEngineBitmex.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(bitmexmd SHARED ${BITMEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitmexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bitmexmd DESTINATION "lib/wingchun")

SET(HITBTC_SOURCE_FILES MDEngineHitBTC.cpp MDEngineHitBTC.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(hitbtcmd SHARED ${HITBTC_SOURCE_FILES})
TARGET_LINK_LIBRARIES(hitbtcmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS hitbtcmd DESTINATION "lib/wingchun")

SET(OCEANEX_SOURCE_FILES MDEngineOceanEx.cpp MDEngineOceanEx.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(oceanexmd SHARED ${OCEANEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(oceanexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS oceanexmd DESTINATION "lib/wingchun")


SET(HUOBI_SOURCE_FILES MDEngineHuobi.cpp MDEngineHuobi.h  IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(huobimd SHARED ${HUOBI_SOURCE_FILES})
TARGET_LINK_LIBRARIES(huobimd ${COMMON_LIBRARIES} ${Boost_LIBRARIES}  ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS huobimd DESTINATION "lib/wingchun")

SET(EMX_SOURCE_FILES MDEngineEmx.cpp MDEngineEmx.h  IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(emxmd SHARED ${EMX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(emxmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES}  ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS emxmd DESTINATION "lib/wingchun")

SET(KRAKEN_SOURCE_FILES MDEngineKraken.cpp MDEngineKraken.h  IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(krakenmd SHARED ${KRAKEN_SOURCE_FILES})
TARGET_LINK_LIBRARIES(krakenmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES}  ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS krakenmd DESTINATION "lib/wingchun")

SET(PROBIT_SOURCE_FILES MDEngineProbit.cpp MDEngineProbit.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(probitmd SHARED ${PROBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(probitmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS probitmd DESTINATION "lib/wingchun")

SET(BITFLYER_SOURCE_FILES MDEnginebitFlyer.cpp MDEnginebitFlyer.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(bitflyermd SHARED ${BITFLYER_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitflyermd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bitflyermd DESTINATION "lib/wingchun")

SET(UPBIT_SOURCE_FILES MDEngineUpbit.cpp MDEngineUpbit.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(upbitmd SHARED ${UPBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(upbitmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS upbitmd DESTINATION "lib/wingchun")

SET(ERISX_SOURCE_FILES MDEngineErisX.cpp MDEngineErisX.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(erisxmd SHARED ${ERISX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(erisxmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS erisxmd DESTINATION "lib/wingchun")

SET(BITHUMB_SOURCE_FILES MDEngineBithumb.cpp MDEngineBithumb.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../base/websocket.h ../base/websocket.cpp)
ADD_LIBRARY(bithumbmd SHARED ${BITHUMB_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bithumbmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bithumbmd DESTINATION "lib/wingchun")

SET(POLONIEX_SOURCE_FILES MDEnginePoloniex.cpp MDEnginePoloniex.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(poloniexmd SHARED ${POLONIEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(poloniexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS poloniexmd DESTINATION "lib/wingchun")

SET(BITSTAMP_SOURCE_FILES MDEngineBitstamp.cpp MDEngineBitstamp.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(bitstampmd SHARED ${BITSTAMP_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bitstampmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bitstampmd DESTINATION "lib/wingchun")


SET(DAYBIT_SOURCE_FILES MDEngineDaybit.cpp MDEngineDaybit.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/Utils.h ../../utils/common/Utils.cpp)
ADD_LIBRARY(daybitmd SHARED ${DAYBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(daybitmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES}  ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS daybitmd DESTINATION "lib/wingchun")

SET(KUCOIN_SOURCE_FILES MDEngineKuCoin.cpp MDEngineKuCoin.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/Utils.h ../../utils/common/Utils.cpp)
ADD_LIBRARY(kucoinmd SHARED ${KUCOIN_SOURCE_FILES})
TARGET_LINK_LIBRARIES(kucoinmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS kucoinmd DESTINATION "lib/wingchun")

SET(KUMEX_SOURCE_FILES MDEngineKumex.cpp MDEngineKumex.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/Utils.h ../../utils/common/Utils.cpp)
ADD_LIBRARY(kumexmd SHARED ${KUMEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(kumexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS kumexmd DESTINATION "lib/wingchun")

SET(DERIBIT_SOURCE_FILES MDEngineDeribit.cpp MDEngineDeribit.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/Utils.h ../../utils/common/Utils.cpp)
ADD_LIBRARY(deribitmd SHARED ${DERIBIT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(deribitmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS deribitmd DESTINATION "lib/wingchun")

SET(BINANCEF_SOURCE_FILES MDEngineBinanceF.cpp MDEngineBinanceF.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/Utils.h ../../utils/common/Utils.cpp)
ADD_LIBRARY(binancefmd SHARED ${BINANCEF_SOURCE_FILES})
TARGET_LINK_LIBRARIES(binancefmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS binancefmd DESTINATION "lib/wingchun")

SET(COINFLEX_SOURCE_FILES MDEngineCoinFlex.cpp MDEngineCoinFlex.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(coinflexmd SHARED ${COINFLEX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coinflexmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coinflexmd DESTINATION "lib/wingchun")

SET(COINFLOOR_SOURCE_FILES MDEngineCoinfloor.cpp MDEngineCoinfloor.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(coinfloormd SHARED ${COINFLOOR_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coinfloormd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coinfloormd DESTINATION "lib/wingchun")

SET(BEQUANT_SOURCE_FILES MDEngineBequant.cpp MDEngineBequant.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(bequantmd SHARED ${BEQUANT_SOURCE_FILES})
TARGET_LINK_LIBRARIES(bequantmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS bequantmd DESTINATION "lib/wingchun")

SET(HBDM_SOURCE_FILES MDEngineHbdm.cpp MDEngineHbdm.h  IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../base/websocket.h ../base/websocket.cpp ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(hbdmmd SHARED ${HBDM_SOURCE_FILES})
TARGET_LINK_LIBRARIES(hbdmmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES}  ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so  ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS hbdmmd DESTINATION "lib/wingchun")

SET(KRAKENF_SOURCE_FILES MDEngineKrakenF.cpp MDEngineKrakenF.h  IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/ld_utils.h ../../utils/common/ld_utils.cpp)
ADD_LIBRARY(krakenfmd SHARED ${KRAKENF_SOURCE_FILES})
TARGET_LINK_LIBRARIES(krakenfmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES}  ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS krakenfmd DESTINATION "lib/wingchun")

SET(COINCHECK_SOURCE_FILES MDEngineCoincheck.cpp MDEngineCoincheck.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/websocket.h ../base/websocket.cpp PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(coincheckmd SHARED ${COINCHECK_SOURCE_FILES})
TARGET_LINK_LIBRARIES(coincheckmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS coincheckmd DESTINATION "lib/wingchun")

SET(FTX_SOURCE_FILES MDEngineFtx.cpp MDEngineFtx.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/websocket.h ../base/websocket.cpp PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(ftxmd SHARED ${FTX_SOURCE_FILES})
TARGET_LINK_LIBRARIES(ftxmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS ftxmd DESTINATION "lib/wingchun")

SET(OKEXF_SOURCE_FILES MDEngineOkexF.cpp MDEngineOkexF.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/websocket.h ../base/websocket.cpp PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(okexfmd SHARED ${OKEXF_SOURCE_FILES})
TARGET_LINK_LIBRARIES(okexfmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS okexfmd DESTINATION "lib/wingchun")

SET(GOOGLETRENDS_SOURCE_FILES MDEngineGoogletrends.cpp MDEngineGoogletrends.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/websocket.h ../base/websocket.cpp PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(googletrendsmd SHARED ${GOOGLETRENDS_SOURCE_FILES})
TARGET_LINK_LIBRARIES(googletrendsmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS googletrendsmd DESTINATION "lib/wingchun")

SET(QDP_SOURCE_FILES MDEngineQDP.cpp MDEngineQDP.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h ../base/websocket.h ../base/websocket.cpp PriceBook20Assembler.cpp PriceBook20Assembler.h)
ADD_LIBRARY(qdpmd SHARED ${QDP_SOURCE_FILES})
TARGET_LINK_LIBRARIES(qdpmd qdpmdapi ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so)
INSTALL(TARGETS qdpmd DESTINATION "lib/wingchun")

SET(BINANCED_SOURCE_FILES MDEngineBinanceD.cpp MDEngineBinanceD.h IMDEngine.cpp IMDEngine.h ../base/IEngine.cpp ../base/IEngine.h ../base/CoinPairWhiteList.cpp ../base/CoinPairWhiteList.h PriceBook20Assembler.cpp PriceBook20Assembler.h ../../utils/common/Utils.h ../../utils/common/Utils.cpp)
ADD_LIBRARY(binancedmd SHARED ${BINANCED_SOURCE_FILES})
TARGET_LINK_LIBRARIES(binancedmd ${COMMON_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcpr.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcurl.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libcrypto.so ${CMAKE_SOURCE_DIR}/utils/cpr/lib/libssl.so ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so ${CMAKE_SOURCE_DIR}/utils/zlib/lib/libz.so.1.2.11)
INSTALL(TARGETS binancedmd DESTINATION "lib/wingchun")

INSTALL(DIRECTORY ${CMAKE_SOURCE_DIR}/utils/cpr/lib/ DESTINATION "lib/wingchun")

INSTALL(DIRECTORY ${CMAKE_SOURCE_DIR}/utils/zlib/lib/ DESTINATION "lib/wingchun")

INSTALL(FILES ${CMAKE_SOURCE_DIR}/utils/libwebsockets/lib/libwebsockets.so.13 DESTINATION "lib/wingchun")

