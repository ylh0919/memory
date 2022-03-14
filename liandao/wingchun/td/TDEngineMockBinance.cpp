#include "TDEngineMockBinance.h"

USING_WC_NAMESPACE

TDEngineMockBinance::TDEngineMockBinance(): TDEngineMockBase("MockBinance",SOURCE_MOCKBINANCE)
{
    KF_LOG_INFO(logger, "[TDEngineMockBinance]");
}

TDEngineMockBinance::~TDEngineMockBinance()
{
}



BOOST_PYTHON_MODULE(libmockbinancetd)
{
    using namespace boost::python;
    class_<TDEngineMockBinance, boost::shared_ptr<TDEngineMockBinance> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMockBinance::initialize)
            .def("start", &TDEngineMockBinance::start)
            .def("stop", &TDEngineMockBinance::stop)
            .def("logout", &TDEngineMockBinance::logout)
            .def("wait_for_stop", &TDEngineMockBinance::wait_for_stop);
}
