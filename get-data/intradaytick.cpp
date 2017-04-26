/* Copyright 2012. Bloomberg Finance L.P.
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to
* deal in the Software without restriction, including without limitation the
* rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
* sell copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:  The above
* copyright notice and this permission notice shall be included in all copies
* or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
* IN THE SOFTWARE.
*/
#include "stdafx.h"

#include <blpapi_session.h>
#include <blpapi_eventdispatcher.h>

#include <blpapi_event.h>
#include <blpapi_message.h>
#include <blpapi_element.h>
#include <blpapi_name.h>
#include <blpapi_request.h>
#include <blpapi_subscriptionlist.h>
#include <blpapi_defs.h>

#include <iomanip>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <stdlib.h>
#include <string.h>
#include <time.h>

using namespace BloombergLP;
using namespace blpapi;

namespace {
	const Name TICK_DATA("tickData");
	const Name TICK_SIZE("size");
	const Name TIME("time");
	const Name TYPE("type");
	const Name VALUE("value");
	const Name RESPONSE_ERROR("responseError");
	const Name CATEGORY("category");
	const Name MESSAGE("message");
	const Name SESSION_TERMINATED("SessionTerminated");
};

class IntradayTickExample {

	std::string                 d_host;
	int                         d_port;
	std::string                 d_security;
	std::vector<std::string>    d_events;
	std::string                 d_startDateTime;
	std::string                 d_endDateTime;
	bool						d_security_assigned;
	bool						d_startDateTime_assigned;
	bool						d_endDateTime_assigned;


	void printUsage()
	{
		std::cout
			<< "Usage:" << '\n'
			<< "  Retrieve intraday rawticks " << '\n'
			<< "    [-s     <security = IBM US Equity>" << '\n'
			<< "    [-e     <event = TRADE/BID/ASK>" << '\n'
			<< "    [-sd    <startDateTime  = 2008-08-11T15:30:00>" << '\n'
			<< "    [-ed    <endDateTime    = 2008-08-11T15:35:00>" << '\n'
			<< "    [-ip    <ipAddress = localhost>" << '\n'
			<< "    [-p     <tcpPort   = 8194>" << '\n'
			<< "Notes:" << '\n'
			<< "1) All times are in GMT." << '\n'
			<< "2) Only one security can be specified." << std::endl;
	}

	void printErrorInfo(const char *leadingStr, const Element &errorInfo)
	{
		std::cout
			<< leadingStr
			<< errorInfo.getElementAsString(CATEGORY)
			<< " (" << errorInfo.getElementAsString(MESSAGE)
			<< ")" << std::endl;
	}

	bool parseCommandLine(int argc, char **argv)
	{
		for (int i = 1; i < argc; ++i) {
			if (!std::strcmp(argv[i], "-s") && i + 1 < argc) {
				d_security = argv[++i];
				d_security_assigned = true;
			}
			else if (!std::strcmp(argv[i], "-e") && i + 1 < argc) {
				d_events.push_back(argv[++i]);
			}
			else if (!std::strcmp(argv[i], "-sd") && i + 1 < argc) {
				d_startDateTime = argv[++i];
				d_startDateTime_assigned = true;
			}
			else if (!std::strcmp(argv[i], "-ed") && i + 1 < argc) {
				d_endDateTime = argv[++i];
				d_endDateTime_assigned = true;
			}
			else if (!std::strcmp(argv[i], "-ip") && i + 1 < argc) {
				d_host = argv[++i];
			}
			else if (!std::strcmp(argv[i], "-p") && i + 1 < argc) {
				d_port = std::atoi(argv[++i]);
				continue;
			}
			else {
				printUsage();
				return false;
			}
		}

		// Add desired events
		if (d_events.size() == 0) {
			d_events.push_back("TRADE");
			d_events.push_back("BID");
			d_events.push_back("ASK");
		}
		return true;
	}

	void processMessage(Message &msg)
	{
		// Extract data from message
		Element data = msg.getElement(TICK_DATA).getElement(TICK_DATA);
		int numItems = data.numValues();

		std::ofstream csvFile(d_security + " " + d_startDateTime + ".csv");
		csvFile << "TIME,TYPE,VALUE,SIZE" << std::endl;

		// Declare variables in each data row
		std::string timeString;
		std::string type;
		double value;
		int size;

		// Outputing necessary data including time/type/price/amount as "item" Element
		for (int i = 0; i < numItems; ++i) {
			Element item = data.getValueAsElement(i);

			timeString = item.getElementAsString(TIME);
			type = item.getElementAsString(TYPE);
			value = item.getElementAsFloat64(VALUE);
			size = item.getElementAsInt32(TICK_SIZE);

			csvFile.setf(std::ios::fixed, std::ios::floatfield);
			csvFile
				<< timeString << ","
				<< type << ","
				<< std::setprecision(3) << std::showpoint << value << ","
				<< std::noshowpoint << size << std::endl;
		}
	}

	void processResponseEvent(Event &event)
	{
		MessageIterator msgIter(event);
		while (msgIter.next()) {
			Message msg = msgIter.message();
			if (msg.hasElement(RESPONSE_ERROR)) {
				printErrorInfo("REQUEST FAILED: ",
					msg.getElement(RESPONSE_ERROR));
				continue;
			}
			processMessage(msg);
		}
	}

	void sendIntradayTickRequest(Session &session)
	{
		Service refDataService = session.getService("//blp/refdata");
		Request request = refDataService.createRequest("IntradayTickRequest");

		// Only one security/eventType per request
		request.set("security", d_security.c_str());

		// Add fields to request
		Element eventTypes = request.getElement("eventTypes");

		for (size_t i = 0; i < d_events.size(); ++i) {
			eventTypes.appendValue(d_events[i].c_str());
		}

		// All times are in GMT
		if (d_startDateTime.empty() || d_endDateTime.empty()) {
			Datetime startDateTime, endDateTime;
			if (0 == getTradingDateRange(&startDateTime, &endDateTime)) {
				request.set("startDateTime", startDateTime);
				request.set("endDateTime", endDateTime);
			}
		}
		else {
			if (!d_startDateTime.empty() && !d_endDateTime.empty()) {
				request.set("startDateTime", d_startDateTime.c_str());
				request.set("endDateTime", d_endDateTime.c_str());
			}
		}

		std::cout << "Sending Request: " << request << std::endl;
		session.sendRequest(request);
	}

	void eventLoop(Session &session)
	{
		bool done = false;
		while (!done) {
			Event event = session.nextEvent();
			if (event.eventType() == Event::PARTIAL_RESPONSE) {
				std::cout << "Processing Partial Response" << std::endl;
				processResponseEvent(event);
			}
			else if (event.eventType() == Event::RESPONSE) {
				std::cout << "Processing Response" << std::endl;
				processResponseEvent(event);
				done = true;
			}
			else {
				MessageIterator msgIter(event);
				while (msgIter.next()) {
					Message msg = msgIter.message();
					if (event.eventType() == Event::SESSION_STATUS) {
						if (msg.messageType() == SESSION_TERMINATED) {
							done = true;
						}
					}
				}
			}
		}
	}

	int getTradingDateRange(Datetime *startDate_p, Datetime *endDate_p)
	{
		struct tm *tm_p;
		time_t currTime = time(0);

		tm_p = (struct tm*) malloc(sizeof(struct tm));

		while (currTime > 0) {
			currTime -= 86400; // GO back one day
			localtime_s(tm_p, &currTime);
			if (tm_p == NULL) {
				break;
			}

			// if not sunday / saturday, assign values & return
			if (tm_p->tm_wday == 0 || tm_p->tm_wday == 6) {// Sun/Sat
				continue;
			}

			startDate_p->setDate(tm_p->tm_year + 1900,
				tm_p->tm_mon + 1,
				tm_p->tm_mday);
			startDate_p->setTime(15, 30, 0);

			endDate_p->setDate(tm_p->tm_year + 1900,
				tm_p->tm_mon + 1,
				tm_p->tm_mday);
			endDate_p->setTime(15, 35, 0);

			free(tm_p);
			return(0);
		}
		free(tm_p);
		return (-1);
	}

	void setConfig()
	{
		if (!d_security_assigned) {
			setSecurity();
		}
		if (!d_startDateTime_assigned) {
			setStartDateTime();
		}
		if (!d_endDateTime_assigned) {
			setEndDateTime();
		}
	}

	void setSecurity()
	{
		std::string x;
		std::cout << "Provide ticker: ";
		std::getline(std::cin, x);
		d_security = x;
	}

	void setStartDateTime()
	{
		std::string x;
		std::cout << "Provide start date: ";
		std::getline(std::cin, x);
		d_startDateTime = x;
	}

	void setEndDateTime()
	{
		std::string x;
		std::cout << "Provide start date: ";
		std::getline(std::cin, x);
		d_endDateTime = x;
	}

public:

	IntradayTickExample()
	{
		d_host = "localhost";
		d_port = 8194;
		d_security_assigned = false;
		d_startDateTime_assigned = false;
		d_endDateTime_assigned = false;
	}

	~IntradayTickExample() {
	}

	void run(int argc, char **argv)
	{
		if (!parseCommandLine(argc, argv)) return;
		setConfig();

		SessionOptions sessionOptions;
		sessionOptions.setServerHost(d_host.c_str());
		sessionOptions.setServerPort(d_port);

		std::cout << "Connecting to " << d_host << ":" << d_port << std::endl;
		Session session(sessionOptions);
		if (!session.start()) {
			std::cerr << "Failed to start session." << std::endl << std::endl;
			return;
		}
		if (!session.openService("//blp/refdata")) {
			std::cerr << "Failed to open //blp/refdata" << std::endl;
			return;
		}

		sendIntradayTickRequest(session);

		// wait for events from session.
		eventLoop(session);

		session.stop();
	}
};

int main(int argc, char **argv)
{
	std::cout << "GGGPA IntraDay Scraper" << std::endl;
	IntradayTickExample example;
	try {
		example.run(argc, argv);
	}
	catch (Exception &e) {
		std::cerr << "Library Exception!!! " << e.description() << std::endl << std::endl;
	}
	// wait for enter key to exit application
	std::cout << "Press ENTER to quit" << std::endl;
	std::string dummy;
	std::getline(std::cin, dummy);
	return 0;
}
