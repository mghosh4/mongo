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
#include <cstdlib>
#include <ctime>
#include "mongo/client/dbclient.h"
#include "mongo/util/time_support.h"

//GENERAL BUILD COMMAND: g++ measureLatency.cpp  -I../../.. (this the mongo src folder) -L[mongo lib folder after build] -I[mongo include folder after build] -lmongoclient -lboost_thread-mt -lboost_filesystem -lboost_system -pthread -o measureLatency
//FOR GOPAL: g++ measureLatency.cpp  -I../../.. -L/home/vgkholla/myBin/lib -I/home/vgkholla/myBin/include -lmongoclient -lboost_thread-mt -lboost_filesystem -lboost_system -pthread -o /home/vgkholla/myBin/otherBin/measureLatency
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

void run(string router, string ns, long long start, long long range, int sleepTime) {
    DBClientConnection c;
    c.connect(router);
    
    struct timeval start_time, stop_time, delay;
    char timeStr[25];
    bool flag;
    BSONObj b;
    srand(time(NULL));
    long long user_id;
    long long number;
    int opSelector;


    BSONObj insertObj;
    BSONObj query;
    BSONObj updateObj;
    BSONObj readObj;

    int numOps = 3; 
    int i = 0;

    string operation = "none";

    map<long long, int> insertedKeys;

    while( true ) {
        flag = false;
        curTimeString(timeStr);
        gettimeofday(&start_time, NULL);

        opSelector = i % numOps;
        i++;
        try {
            switch(opSelector) {
                case 0: //insert
                        operation = "insert";
                        while(true) {
                            user_id = start + (rand() % range);
                            if( insertedKeys.find(user_id) == insertedKeys.end()) { //key not been inserted previously
                                insertedKeys.insert(make_pair(user_id, 1));
                                cout<<operation<<": Info: inserting " << user_id << endl;
                                break;
                            } 
                        }
                        //insert command goes here
                        number = 2 * start + range - user_id; 
                        insertObj = BSON("user_id" << user_id << "number" << number << "name" << "name");
                        //cout<<"insert: "<<insertObj.toString()<<endl;
                        c.insert(ns, insertObj);
                    break;
                case 1: //update
                        operation = "update";
                        //update command goes here
                        query = BSON("user_id" << user_id);
                        updateObj = BSON("user_id" << user_id << "number" << number << "name" << "nameUpdated");
                        //cout<<"update: "<<updateObj.toString()<<endl;
                        c.update(ns, Query(query), updateObj);
                    break;
                case 2:
                        //read
                        operation = "read";
                        readObj = BSON("user_id" << user_id);
                        //cout<<"read: "<<readObj.toString()<<endl;
                        b = c.findOne(ns, Query(readObj), 0, QueryOption_SlaveOk);
                    break;
                default:
                    cout<<"Unrecognized opSelector ! " << opSelector << endl;
                    cout<<"i : " << i << " numOps : " << numOps << endl;
                    break; 
            }
        } catch (DBException e){
            flag = true;
            cout << "Error: " << e.toString() << endl;
        }

        cout<<operation<<": ";
        if (!flag) {
            gettimeofday(&stop_time, NULL);
            delay = subtract(start_time, stop_time);
            cout << timeStr << ": " << delay.tv_sec*1000 + delay.tv_usec/(double)1000 << endl;
        } else {
            cout<<"Failed: Error logged"; 
        }

        usleep(sleepTime);
    }
}

int main(int argc, char* argv[]) {

    if (argc < 5)
    {
        cout << "Program takes 5 arguments: QueryRouter (IP:port), Namespace, Start, Range, Sleep time between operations" << endl;
        exit(0);
    }

    try {
        run(argv[1], argv[2], atol(argv[3]), atol(argv[4]), atoi(argv[5]));
    }
    catch( DBException &e ) {
        cout << "caught " << e.what() << endl;
    }
    return 0;
}
