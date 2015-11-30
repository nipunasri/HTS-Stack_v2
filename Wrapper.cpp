#include <iostream>
#include <fstream>
#include "HTSOcl.hpp"
#include "HTSFrontEnd.hpp"


int main(int argc, char *argv[])
	{
	//get OCL context
	TOclContext tOclContext;


	// HTSOcl.cpp
	if (uiGetOCLContext(&tOclContext) != HTS_OK)
		{
		std::cout << "failed to initialize OCL context" << std::endl;
		return -1;
		}

	//get the front end 
	CFrontEnd tFrontEnd;

	//bind context
	if(tFrontEnd.uiBindOCLContext(&tOclContext) != HTS_OK)
		{
		std::cout << "failed to bind OCL context." << std::endl;
		return -1;
		}

	//build OCL kernels
	if(tFrontEnd.uiBuildOCLKernels() != HTS_OK)
		{
		std::cout << "failed to build OCL kernels." << std::endl;
		getchar() ;
		return -1;
		}

	//start front end processing --> create host-thread for submitting commands
	tFrontEnd.uiOpenFrontEnd(); // this has to be one, which is resposible for launching kernel threads
	
	//register the thread 
	TFid tFid = tFrontEnd.tRegister();


	cl_uint uiNoThreads = tFrontEnd.uiGetThreadCount();

	std::cout << "Threads:" << uiNoThreads << std::endl;

	//submit some requests
	CRequest tReq;
	TEvent   tReqEvent;

	tReq.uiType = HTS_REQ_TYPE_FIND;
	tReq.uiKey = 1024*600; 
	
	if(tFrontEnd.uiSubmitReq(tFid, tReq, tReqEvent) == HTS_OK)
		{
		cl_uint   uiStatus;
		cl_uint   uiReqStatus;

		//std::cout << "waiting on:" << std::endl;
		//std::cout << (long)(tFid) << ":";
		//std::cout << tReqEvent << ":";
		//std::cout << ((tFid->pThreadRequest[tReqEvent]).uiFlags) << std::endl;

		uiReqStatus = tFrontEnd.uiGetStatus(tFid,tReqEvent,&uiStatus);
		while (uiReqStatus == HTS_NOT_OK)
			{
			uiReqStatus = tFrontEnd.uiGetStatus(tFid,tReqEvent,&uiStatus);
			}

		if(uiReqStatus == HTS_REQ_COMPLETED)
			std::cout << "request is successful: " << uiStatus << std::endl;
		}
	
	tReq.uiType = HTS_REQ_TYPE_FIND; 
	tReq.uiKey = 1024*10;

	if(tFrontEnd.uiSubmitReq(tFid, tReq, tReqEvent) == HTS_OK)
		{
		cl_uint   uiStatus;
		cl_uint   uiReqStatus;

		//std::cout << "waiting on:" << std::endl;
		//std::cout << (long)(tFid) << ":";
		//std::cout << tReqEvent << ":";
		//std::cout << ((tFid->pThreadRequest[tReqEvent]).uiFlags) << std::endl;

		uiReqStatus = tFrontEnd.uiGetStatus(tFid,tReqEvent,&uiStatus);
		while (uiReqStatus == HTS_NOT_OK)
			{
			uiReqStatus = tFrontEnd.uiGetStatus(tFid,tReqEvent,&uiStatus);
			}
			if(uiReqStatus == HTS_REQ_COMPLETED)
			std::cout << "request is successful: " << uiStatus << std::endl;
	}
	
		
	tFrontEnd.uiDeRegister(tFid);
	//std::cout << "waiting for front end thread to finish." << std::endl;
	tFrontEnd.uiCloseFrontEnd();


	tFrontEnd.uiReleaseOCLKernels();

	uiReleaseOCLContext(&tOclContext);
	getchar() ;
	return 0;
	}
