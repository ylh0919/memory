#include "TDEngineMockCoinflex.h"

USING_WC_NAMESPACE

TDEngineMockCoinflex::TDEngineMockCoinflex(): TDEngineMockBase("MockCoinflex",SOURCE_MOCKCOINFLEX)
{
    KF_LOG_INFO(logger, "[TDEngineMockCoinflex]");
}

TDEngineMockCoinflex::~TDEngineMockCoinflex()
{
}



BOOST_PYTHON_MODULE(libmockcoinflextd)
{
    using namespace boost::python;
    class_<TDEngineMockCoinflex, boost::shared_ptr<TDEngineMockCoinflex> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMockCoinflex::initialize)
            .def("start", &TDEngineMockCoinflex::start)
            .def("stop", &TDEngineMockCoinflex::stop)
            .def("logout", &TDEngineMockCoinflex::logout)
            .def("wait_for_stop", &TDEngineMockCoinflex::wait_for_stop);
}
