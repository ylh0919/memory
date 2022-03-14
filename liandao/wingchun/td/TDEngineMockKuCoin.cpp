#include "TDEngineMockKuCoin.h"

USING_WC_NAMESPACE

TDEngineMockKuCoin::TDEngineMockKuCoin(): TDEngineMockBase("MockKucoin",SOURCE_MOCKKUCOIN)
{
    KF_LOG_INFO(logger, "[TDEngineMockKuCoin]");
}

TDEngineMockKuCoin::~TDEngineMockKuCoin()
{
}



BOOST_PYTHON_MODULE(libmockkucointd)
{
    using namespace boost::python;
    class_<TDEngineMockKuCoin, boost::shared_ptr<TDEngineMockKuCoin> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMockKuCoin::initialize)
            .def("start", &TDEngineMockKuCoin::start)
            .def("stop", &TDEngineMockKuCoin::stop)
            .def("logout", &TDEngineMockKuCoin::logout)
            .def("wait_for_stop", &TDEngineMockKuCoin::wait_for_stop);
}
