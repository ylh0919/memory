#include "TDEngineMockHbdm.h"

USING_WC_NAMESPACE

TDEngineMockHbdm::TDEngineMockHbdm(): TDEngineMockBase("MockHbdm",SOURCE_MOCKHBDM)
{
    KF_LOG_INFO(logger, "[TDEngineMockHbdm]");
}

TDEngineMockHbdm::~TDEngineMockHbdm()
{
}



BOOST_PYTHON_MODULE(libmockhbdmtd)
{
    using namespace boost::python;
    class_<TDEngineMockHbdm, boost::shared_ptr<TDEngineMockHbdm> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMockHbdm::initialize)
            .def("start", &TDEngineMockHbdm::start)
            .def("stop", &TDEngineMockHbdm::stop)
            .def("logout", &TDEngineMockHbdm::logout)
            .def("wait_for_stop", &TDEngineMockHbdm::wait_for_stop);
}
