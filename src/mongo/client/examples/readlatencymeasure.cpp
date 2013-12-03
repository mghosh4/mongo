//readlatencymeasure.cpp

/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include <iostream>
#include "mongo/client/dbclient.h"
#include "mongo/util/time_support.h"

// g++ src/mongo/client/examples/readlatencymeasure.cpp -pthread -Isrc -Isrc/mongo -lmongoclient -lboost_thread-mt -lboost_system -lboost_filesystem -L[path to libmongoclient.a] -o readlatencymeasure
// g++ readlatencymeasure.cpp -L[mongo directory] -L/opt/local/lib -lmongoclient -lboost_thread-mt -lboost_filesystem -lboost_system -I/opt/local/include  -o readlatencymeasure

using namespace mongo;

struct timeval subtract(struct timeval start_time, struct timeval stop_time) {
	struct timeval delay;
	/* Perform the carry for the later subtraction by updating y. */
	if (stop_time.tv_usec < start_time.tv_usec) {
		int nsec = (start_time.tv_usec - stop_time.tv_usec) / 1000000 + 1;
		start_time.tv_usec -= 1000000 * nsec;
		start_time.tv_sec += nsec;
	}
	if (stop_time.tv_usec - start_time.tv_usec > 1000000) {
		int nsec = (stop_time.tv_usec - start_time.tv_usec) / 1000000;
		start_time.tv_usec += 1000000 * nsec;
		start_time.tv_sec -= nsec;
	}

	/* Compute the time remaining to wait.
	 tv_usec is certainlstart_time positive. */
	delay.tv_sec = stop_time.tv_sec - start_time.tv_sec;
	delay.tv_usec = stop_time.tv_usec - start_time.tv_usec;
	return delay;
}

void run(string ns) {
    DBClientConnection c;
    c.connect("localhost"); //"192.168.58.1");
    cout << "connected ok" << endl;
	struct timeval start_time, stop_time, delay;
	char timeStr[25];

    while( true ) {
		curTimeString(timeStr);
		gettimeofday(&start_time, NULL);
		try {
			BSONObj b = c.findOne(ns, Query(), 0, QueryOption_SlaveOk);
		}
		catch (DBException e){
			cout << "Error:" << e.toString() << endl;
		}

		gettimeofday(&stop_time, NULL);
		//cout << "Returned result:" << b.toString();

		delay = subtract(start_time, stop_time);
		cout << timeStr << " " << delay.tv_sec*1000 + delay.tv_usec/(double)1000 << endl;
		usleep(100000);
    }
}

int main(int argc, char* argv[]) {

	if (argc < 2)
	{
		cout << "Collection Name not provided" << endl;
		exit(0);
	}

    try {
        run(argv[1]);
    }
    catch( DBException &e ) {
        cout << "caught " << e.what() << endl;
    }
    return 0;
}
