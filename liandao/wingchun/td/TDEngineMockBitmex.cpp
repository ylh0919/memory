#include "TDEngineMockBitmex.h"

USING_WC_NAMESPACE

TDEngineMockBitmex::TDEngineMockBitmex(): TDEngineMockBase("MockBitmex",SOURCE_MOCKBITMEX)
{
    KF_LOG_INFO(logger, "[TDEngineMockBitmex]");
}

TDEngineMockBitmex::~TDEngineMockBitmex()
{
}



BOOST_PYTHON_MODULE(libmockbitmextd)
{
    using namespace boost::python;
    class_<TDEngineMockBitmex, boost::shared_ptr<TDEngineMockBitmex> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMockBitmex::initialize)
            .def("start", &TDEngineMockBitmex::start)
            .def("stop", &TDEngineMockBitmex::stop)
            .def("logout", &TDEngineMockBitmex::logout)
            .def("wait_for_stop", &TDEngineMockBitmex::wait_for_stop);
}
