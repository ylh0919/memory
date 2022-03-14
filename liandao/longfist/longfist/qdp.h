#ifndef LONGFIST_UTIL_QDP_H
#define LONGFIST_UTIL_QDP_H

#include "../api/qdp/Qdp2.1.9_api_Linux_7.2_20200818/include/QdpFtdcUserApiStruct.h"
#include "LFDataStruct.h"
#define KUNGFU_LF_QDP
#include "longfist/transfer_m.h"

inline struct LFMarketDataField parseFrom(const struct CQdpFtdcDepthMarketDataField & ori)
{
	struct LFMarketDataField res = {0};
	memcpy(res.TradingDay, ori.TradingDay, 9);
	memcpy(res.InstrumentID, ori.InstrumentID, 31);
	memcpy(res.ExchangeID, ori.ExchangeID, 11);				//
	//memcpy(res.ExchangeInstID, ori.ExchangeInstID, 64);	//没有
	memcpy(res.UpdateTime, ori.UpdateTime, 9);
	res.LastPrice = ori.LastPrice;
	res.PreSettlementPrice = ori.PreSettlementPrice;
	res.PreClosePrice = ori.PreClosePrice;
	res.PreOpenInterest = ori.PreOpenInterest;
	res.OpenPrice = ori.OpenPrice;
	res.HighestPrice = ori.HighestPrice;
	res.LowestPrice = ori.LowestPrice;
	res.Volume = ori.Volume;
	res.Turnover = ori.Turnover;
	res.OpenInterest = ori.OpenInterest;
	res.ClosePrice = ori.ClosePrice;
	res.SettlementPrice = ori.SettlementPrice;
	res.UpperLimitPrice = ori.UpperLimitPrice;
	res.LowerLimitPrice = ori.LowerLimitPrice;
	res.PreDelta = ori.PreDelta;
	res.CurrDelta = ori.CurrDelta;
	res.UpdateMillisec = ori.UpdateMillisec;
	res.BidPrice1 = ori.BidPrice1;
	res.BidVolume1 = ori.BidVolume1;
	res.AskPrice1 = ori.AskPrice1;
	res.AskVolume1 = ori.AskVolume1;
	res.BidPrice2 = ori.BidPrice2;
	res.BidVolume2 = ori.BidVolume2;
	res.AskPrice2 = ori.AskPrice2;
	res.AskVolume2 = ori.AskVolume2;
	res.BidPrice3 = ori.BidPrice3;
	res.BidVolume3 = ori.BidVolume3;
	res.AskPrice3 = ori.AskPrice3;
	res.AskVolume3 = ori.AskVolume3;
	res.BidPrice4 = ori.BidPrice4;
	res.BidVolume4 = ori.BidVolume4;
	res.AskPrice4 = ori.AskPrice4;
	res.AskVolume4 = ori.AskVolume4;
	res.BidPrice5 = ori.BidPrice5;
	res.BidVolume5 = ori.BidVolume5;
	res.AskPrice5 = ori.AskPrice5;
	res.AskVolume5 = ori.AskVolume5;
	return res;
}

inline struct LFMarketDataField parsewWithScale_offset(const struct CQdpFtdcDepthMarketDataField& ori)
{
	const int scale_offset = 1e8;
	struct LFMarketDataField res = { 0 };
	memcpy(res.TradingDay, ori.TradingDay, 9);
	memcpy(res.InstrumentID, ori.InstrumentID, 31);
	memcpy(res.ExchangeID, ori.ExchangeID, 11);				//
	//memcpy(res.ExchangeInstID, ori.ExchangeInstID, 64);	//没有
	memcpy(res.UpdateTime, ori.UpdateTime, 9);
	res.LastPrice = ori.LastPrice;
	res.PreSettlementPrice = ori.PreSettlementPrice;
	res.PreClosePrice = ori.PreClosePrice;
	res.PreOpenInterest = ori.PreOpenInterest;
	res.OpenPrice = ori.OpenPrice;
	res.HighestPrice = ori.HighestPrice;
	res.LowestPrice = ori.LowestPrice;
	res.Volume = ori.Volume;
	res.Turnover = ori.Turnover;
	res.OpenInterest = ori.OpenInterest;
	res.ClosePrice = ori.ClosePrice;
	res.SettlementPrice = ori.SettlementPrice;
	res.UpperLimitPrice = ori.UpperLimitPrice;
	res.LowerLimitPrice = ori.LowerLimitPrice;
	res.PreDelta = ori.PreDelta;
	res.CurrDelta = ori.CurrDelta;
	res.UpdateMillisec = ori.UpdateMillisec;
	res.BidPrice1 = ori.BidPrice1 * scale_offset;
	res.BidVolume1 = ori.BidVolume1 * scale_offset;
	res.AskPrice1 = ori.AskPrice1 * scale_offset;
	res.AskVolume1 = ori.AskVolume1 * scale_offset;
	res.BidPrice2 = ori.BidPrice2 * scale_offset;
	res.BidVolume2 = ori.BidVolume2 * scale_offset;
	res.AskPrice2 = ori.AskPrice2 * scale_offset;
	res.AskVolume2 = ori.AskVolume2 * scale_offset;
	res.BidPrice3 = ori.BidPrice3 * scale_offset;
	res.BidVolume3 = ori.BidVolume3 * scale_offset;
	res.AskPrice3 = ori.AskPrice3 * scale_offset;
	res.AskVolume3 = ori.AskVolume3 * scale_offset;
	res.BidPrice4 = ori.BidPrice4 * scale_offset;
	res.BidVolume4 = ori.BidVolume4 * scale_offset;
	res.AskPrice4 = ori.AskPrice4 * scale_offset;
	res.AskVolume4 = ori.AskVolume4 * scale_offset;
	res.BidPrice5 = ori.BidPrice5 * scale_offset;
	res.BidVolume5 = ori.BidVolume5 * scale_offset;
	res.AskPrice5 = ori.AskPrice5 * scale_offset;
	res.AskVolume5 = ori.AskVolume5 * scale_offset;
	return res;
}

inline struct CQdpFtdcQryInvestorPositionField parseTo(const struct LFQryPositionField& ori) {
	struct CQdpFtdcQryInvestorPositionField res = { 0 };
	strncpy(res.BrokerID, ori.BrokerID, sizeof(res.BrokerID));
	strncpy(res.InvestorID, ori.InvestorID, sizeof(res.InvestorID));
	strncpy(res.UserID, ori.InvestorID, sizeof(res.UserID));
	strncpy(res.InstrumentID, ori.InstrumentID, sizeof(res.InstrumentID));
	//strncpy(res.ExchangeID, ori.ExchangeID, sizeof(res.ExchangeID));// ExchangeID may be "qdp", please check it
	return res;
}

inline struct CQdpFtdcQryInvestorAccountField parseTo(const struct LFQryAccountField& ori) {
	struct CQdpFtdcQryInvestorAccountField res = { 0 };
	strncpy(res.BrokerID, ori.BrokerID, sizeof(res.BrokerID));
	strncpy(res.InvestorID, ori.InvestorID, sizeof(res.InvestorID));
	strncpy(res.UserID, ori.InvestorID, sizeof(res.UserID));
	return res;
}

struct CQdpFtdcInputOrderField parseTo(const struct LFInputOrderField& lf)
{
	struct CQdpFtdcInputOrderField res = { 0 };
	strncpy(res.UserID, lf.UserID, sizeof(res.UserID));
	strncpy(res.BrokerID, lf.BrokerID, sizeof(res.BrokerID));
	strncpy(res.InvestorID, lf.InvestorID, sizeof(res.InvestorID));
	strncpy(res.BusinessUnit, lf.BusinessUnit, sizeof(res.BusinessUnit));
	strncpy(res.ExchangeID, lf.ExchangeID, sizeof(res.ExchangeID));
	strncpy(res.InstrumentID, lf.InstrumentID, sizeof(res.InstrumentID));
	
	res.LimitPrice = lf.LimitPrice;
	res.Direction = lf.Direction;
	if (lf.OffsetFlag == LF_CHAR_ForceOff || lf.OffsetFlag == LF_CHAR_LocalForceClose || lf.OffsetFlag == LF_CHAR_Non) { //强减'5' || 本地强平'6' ||不分开平'N'		      
		res.OffsetFlag = QDP_FTDC_OF_Open; //-->开仓'0'
	}
	else
		res.OffsetFlag = lf.OffsetFlag;
	res.Volume = lf.Volume;
	res.OrderPriceType = lf.OrderPriceType;				//隐藏限价'4' --> 五档价'4'
	res.HedgeFlag = lf.HedgeFlag;
	res.TimeCondition = lf.TimeCondition;
	if (lf.TimeCondition == LF_CHAR_FAK || lf.TimeCondition == LF_CHAR_FOK) { 
		res.TimeCondition = QDP_FTDC_TC_IOC;
	}
	res.VolumeCondition = lf.VolumeCondition;
	res.MinVolume = lf.MinVolume;
	res.ForceCloseReason = lf.ForceCloseReason;
	if (lf.ForceCloseReason == LF_CHAR_Violation || lf.ForceCloseReason == LF_CHAR_Other || lf.ForceCloseReason == LF_CHAR_PersonDeliv) { //违规'5' || 其它'6' || 自然人临近交割 '7'
		res.ForceCloseReason = QDP_FTDC_FCR_NotForceClose; //-->非强平'0'
	}
	res.StopPrice = lf.StopPrice;
	res.IsAutoSuspend = lf.IsAutoSuspend;
	strncpy(res.UserOrderLocalID, lf.OrderRef, sizeof(res.UserOrderLocalID));
	//res.RecNum = lf.RequestID;
	return res;
}

struct CQdpFtdcOrderActionField parseTo(const struct LFOrderActionField& lf) {
	struct CQdpFtdcOrderActionField res = { 0 };
	strncpy(res.UserID, lf.UserID, sizeof(res.UserID));
	strncpy(res.BrokerID, lf.BrokerID, sizeof(res.BrokerID));
	strncpy(res.InvestorID, lf.InvestorID, sizeof(res.InvestorID));
	//strncpy(res.BusinessUnit, lf.BusinessUnit, sizeof(res.BusinessUnit));
	strncpy(res.ExchangeID, lf.ExchangeID, sizeof(res.ExchangeID));
	strncpy(res.InstrumentID, lf.InstrumentID, sizeof(res.InstrumentID));
	strncpy(res.OrderSysID, lf.OrderSysID, sizeof(res.OrderSysID));
	strncpy(res.UserOrderActionLocalID, lf.OrderRef, sizeof(res.UserOrderActionLocalID));

	//res.RecNum = lf.RequestID;
	res.ActionFlag = lf.ActionFlag;//
	res.LimitPrice = lf.LimitPrice;
	res.VolumeChange = lf.VolumeChange;
	return res;
}

inline struct LFInputOrderField parseFrom(const struct CQdpFtdcInputOrderField& ori)
{
	struct LFInputOrderField res = { 0 };
	memcpy(res.BrokerID, ori.BrokerID, 11);
	memcpy(res.UserID, ori.UserID, 16);
	memcpy(res.InvestorID, ori.InvestorID, 13);
	memcpy(res.BusinessUnit, ori.BusinessUnit, 21);
	memcpy(res.ExchangeID, ori.ExchangeID, 9);
	memcpy(res.InstrumentID, ori.InstrumentID, 31);
	//memcpy(res.OrderRef, ori.OrderRef, 13);
	memcpy(res.OrderRef, ori.UserOrderLocalID, 13);
	res.LimitPrice = ori.LimitPrice;
	res.Volume = ori.Volume;
	res.MinVolume = ori.MinVolume;
	res.TimeCondition = ori.TimeCondition;
	res.VolumeCondition = ori.VolumeCondition;
	res.OrderPriceType = ori.OrderPriceType;
	res.Direction = ori.Direction;
	res.OffsetFlag = ori.OffsetFlag;
	res.HedgeFlag = ori.HedgeFlag;
	res.ForceCloseReason = ori.ForceCloseReason;
	res.StopPrice = ori.StopPrice;
	res.IsAutoSuspend = ori.IsAutoSuspend;
	//res.ContingentCondition = ori.ContingentCondition;
	res.ContingentCondition = '1';
	return res;
}

inline struct LFOrderActionField parseFrom(const struct CQdpFtdcOrderActionField& ori)
{
	struct LFOrderActionField res = { 0 };
	memcpy(res.BrokerID, ori.BrokerID, 11);
	memcpy(res.InvestorID, ori.InvestorID, 13);
	memcpy(res.InstrumentID, ori.InstrumentID, 31);
	memcpy(res.ExchangeID, ori.ExchangeID, 9);
	memcpy(res.UserID, ori.UserID, 16);
	//memcpy(res.OrderRef, ori.OrderRef, 13);
	memcpy(res.OrderRef, ori.UserOrderLocalID, 13);
	memcpy(res.OrderSysID, ori.OrderSysID, 21);
	//res.RequestID = ori.RequestID;
	//res.RequestID = ori.RecNum;
	res.ActionFlag = ori.ActionFlag;
	res.LimitPrice = ori.LimitPrice;
	res.VolumeChange = ori.VolumeChange;
	return res;
}

inline struct LFRspPositionField parseFrom(const struct CQdpFtdcRspInvestorPositionField& ori)
{
	struct LFRspPositionField res = {0};
	memcpy(res.InstrumentID, ori.InstrumentID, 31);
	//res.YdPosition = (int)(ori.Position - ori.TodayPosition);
	res.YdPosition = ori.YdPosition;
	res.Position = ori.Position;
	memcpy(res.BrokerID, ori.BrokerID, 11);
	memcpy(res.InvestorID, ori.InvestorID, 13);
	res.PositionCost = ori.PositionCost;
	res.HedgeFlag = ori.HedgeFlag;
	//res.PosiDirection = ori.PosiDirection;
	return res;
}

inline struct LFRtnOrderField parseFrom(const struct CQdpFtdcOrderField& ori)
{
	struct LFRtnOrderField res = {0};
	memcpy(res.BrokerID, ori.BrokerID, 11);
	memcpy(res.UserID, ori.UserID, 16);
	memcpy(res.ParticipantID, ori.ParticipantID, 11);
	memcpy(res.InvestorID, ori.InvestorID, 13);
	memcpy(res.BusinessUnit, ori.BusinessUnit, 21);
	memcpy(res.InstrumentID, ori.InstrumentID, 31);
	///用户本地报单号
	//TQdpFtdcUserOrderLocalIDType	UserOrderLocalID;
	//本地报单编号
	//TQdpFtdcOrderLocalIDType	OrderLocalID;
	memcpy(res.OrderRef, ori.UserOrderLocalID, 13);
	memcpy(res.ExchangeID, ori.ExchangeID, 9);
	res.LimitPrice = ori.LimitPrice;
	res.VolumeTraded = ori.VolumeTraded;
	res.VolumeTotal = ori.VolumeRemain;
	res.VolumeTotalOriginal = ori.Volume;
	res.TimeCondition = ori.TimeCondition;
	res.VolumeCondition = ori.VolumeCondition;
	res.OrderPriceType = ori.OrderPriceType;
	res.Direction = ori.Direction;
	res.OffsetFlag = ori.OffsetFlag;
	res.HedgeFlag = ori.HedgeFlag;
	res.OrderStatus = ori.OrderStatus;
	//res.RequestID = ori.RecNum;
	return res;
}

inline struct LFRtnTradeField parseFrom(const struct CQdpFtdcTradeField& ori)
{
	struct LFRtnTradeField res = {0};
	memcpy(res.BrokerID, ori.BrokerID, 11);
	memcpy(res.UserID, ori.UserID, 16);
	memcpy(res.InvestorID, ori.InvestorID, 13);
	//memcpy(res.BusinessUnit, ori.BusinessUnit, 21);
	memcpy(res.InstrumentID, ori.InstrumentID, 31);
	memcpy(res.OrderRef, ori.UserOrderLocalID, 13);
	memcpy(res.ExchangeID, ori.ExchangeID, 9);
	memcpy(res.TradeID, ori.TradeID, 21);
	memcpy(res.OrderSysID, ori.OrderSysID, 21);
	memcpy(res.ParticipantID, ori.ParticipantID, 11);
	memcpy(res.ClientID, ori.ClientID, 11);
	res.Price = ori.TradePrice;
	res.Volume = ori.TradeVolume;
	memcpy(res.TradingDay, ori.TradingDay, 13);
	memcpy(res.TradeTime, ori.TradeTime, 9);
	res.Direction = ori.Direction;
	res.OffsetFlag = ori.OffsetFlag;
	res.HedgeFlag = ori.HedgeFlag;
	return res;
}

inline struct LFRspAccountField parseFrom(const struct CQdpFtdcRspInvestorAccountField& ori)
{
	struct LFRspAccountField res = {0};
	memcpy(res.BrokerID, ori.BrokerID, 11);
	memcpy(res.InvestorID, ori.AccountID, 13);
	//res.PreMortgage = ori.PreMortgage;
	//res.PreCredit = ori.PreCredit;
	//res.PreDeposit = ori.PreDeposit;
	res.preBalance = ori.PreBalance;
	//res.PreMargin = ori.PreMargin;
	res.Deposit = ori.Deposit;
	res.Withdraw = ori.Withdraw;
	res.FrozenMargin = ori.FrozenMargin;
	//res.FrozenCash = ori.FrozenCash;
	res.FrozenCash = ori.FrozenPremium;
	//res.FrozenCommission = ori.FrozenCommission;
	res.FrozenCommission = ori.FrozenFee;
	//res.CurrMargin = ori.CurrMargin;
	//res.CashIn = ori.CashIn;
	//res.Commission = ori.Commission;
	res.CloseProfit = ori.CloseProfit;
	res.PositionProfit = ori.PositionProfit;
	res.Balance = ori.Balance;
	res.Available = ori.Available;
	//res.WithdrawQuota = ori.WithdrawQuota;
	//res.Reserve = ori.Reserve;
	//memcpy(res.TradingDay, ori.TradingDay, 9);
	//res.Credit = ori.Credit;
	res.Mortgage = ori.Mortgage;
	//res.ExchangeMargin = ori.ExchangeMargin;
	//res.DeliveryMargin = ori.DeliveryMargin;
	//res.ExchangeDeliveryMargin = ori.ExchangeDeliveryMargin;
	//res.ReserveBalance = ori.ReserveBalance;
	//res.Equity = ori.PreBalance - ori.PreCredit - ori.PreMortgage + ori.Mortgage - ori.Withdraw + ori.Deposit + ori.CloseProfit + ori.PositionProfit + ori.CashIn - ori.Commission;
	return res;
}

#endif
