#include "TDEngineMock.h"
USING_WC_NAMESPACE

TDEngineMock::TDEngineMock(): TDEngineMockBase("MOCK",SOURCE_MOCK)
{
}
TDEngineMock::~TDEngineMock()
{
}
BOOST_PYTHON_MODULE(libmocktd)
{
    using namespace boost::python;
    class_<TDEngineMock, boost::shared_ptr<TDEngineMock> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMock::initialize)
            .def("start", &TDEngineMock::start)
            .def("stop", &TDEngineMock::stop)
            .def("logout", &TDEngineMock::logout)
            .def("wait_for_stop", &TDEngineMock::wait_for_stop);
}
