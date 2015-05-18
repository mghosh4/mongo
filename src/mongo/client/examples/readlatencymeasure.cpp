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
//Illinois Open Source License
//
//University of Illinois
//Open Source License
//
//Copyright © 2014,    Board of Trustees of the University of Illinois.  All rights reserved.
//
//Developed by:
//
// Distributed Protocols Research Group in the Department of Computer Science
// The University of Illinois at Urbana-Champaign
// http://dprg.cs.uiuc.edu/
// This is for the Project Morphus. The paper can be found at the website http://dprg.cs.uiuc.edu
//Mainak Ghosh, mghosh4@illinois.edu
//Wenting Wang, wwang84@illinois.edu
//Gopalakrishna Holla, vgkholla@gmail.com
//Indranil Gupta, indy@cs.uiuc.edu
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal with the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
//    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
//    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimers in the documentation and/or other materials provided with the distribution.
//    * Neither the names of The Distributed Protocols Research Group (DPRG) or The University of Illinois at Urbana-Champaign, nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
//PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
//AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.

#include <iostream>
#include <cstdlib>
#include <ctime>
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

void run(string ns, long long range) {
    DBClientConnection c;
    c.connect("localhost"); //"192.168.58.1");
    //cout << "connected ok" << endl;
	struct timeval start_time, stop_time, delay;
	char timeStr[25];
    bool flag;
    BSONObj b;
    srand(time(NULL));
    long long user_id;

    while( true ) {
        flag = false;
		curTimeString(timeStr);
		gettimeofday(&start_time, NULL);
		try {
            user_id = rand() % range;
		    b = c.findOne(ns, Query(BSON("user_id" << user_id)), 0, QueryOption_SlaveOk);
		}
		catch (DBException e){
            flag = true;
			cout << "Error:" << e.toString() << endl;
		}

        if (!flag)
        {
		    gettimeofday(&stop_time, NULL);
		    cout << "Returned result:" << b.toString() << endl;

		    delay = subtract(start_time, stop_time);
		    cout << timeStr << " " << delay.tv_sec*1000 + delay.tv_usec/(double)1000 << endl;
        }
        else
        {
            cout << "Returned result: FAILED" << endl;
		    cout << timeStr << " " << INT_MAX << endl;
        }

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
        run(argv[1], atol(argv[2]));
    }
    catch( DBException &e ) {
        cout << "caught " << e.what() << endl;
    }
    return 0;
}
