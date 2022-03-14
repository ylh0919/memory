#include "TDEngineMockDeribit.h"

USING_WC_NAMESPACE

TDEngineMockDeribit::TDEngineMockDeribit(): TDEngineMockBase("MockDeribit",SOURCE_MOCKDERIBIT)
{
    KF_LOG_INFO(logger, "[TDEngineMockDeribit]");
}

TDEngineMockDeribit::~TDEngineMockDeribit()
{
}



BOOST_PYTHON_MODULE(libmockderibittd)
{
    using namespace boost::python;
    class_<TDEngineMockDeribit, boost::shared_ptr<TDEngineMockDeribit> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMockDeribit::initialize)
            .def("start", &TDEngineMockDeribit::start)
            .def("stop", &TDEngineMockDeribit::stop)
            .def("logout", &TDEngineMockDeribit::logout)
            .def("wait_for_stop", &TDEngineMockDeribit::wait_for_stop);
}
