//tutorial.cpp

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
//Copyright © 2015,    Board of Trustees of the University of Illinois.  All rights reserved.
//
//Developed by:
//
// Distributed Protocols Research Group in the Department of Computer Science
// The University of Illinois at Urbana-Champaign
// http://dprg.cs.uiuc.edu/

//Mainak Ghosh, mghosh4@illinois.edu
//Wenting Wang, wwang84@illinois.edu
//Gopalakrishna Holla, hollava2@illinois.edu
//Indranil Gupta, indy@cs.uiuc.edu
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal with the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
//    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
//    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimers in the documentation and/or other materials provided with the distribution.
//    * Neither the names of The Monet Group or The University of Illinois at Urbana-Champaign, nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
//PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
//AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.

#include <iostream>
#include "mongo/client/dbclient.h"

// g++ src/mongo/client/examples/tutorial.cpp -pthread -Isrc -Isrc/mongo -lmongoclient -lboost_thread-mt -lboost_system -lboost_filesystem -L[path to libmongoclient.a] -o tutorial
// g++ tutorial.cpp -L[mongo directory] -L/opt/local/lib -lmongoclient -lboost_thread-mt -lboost_filesystem -lboost_system -I/opt/local/include  -o tutorial

using namespace mongo;

void printIfAge(DBClientConnection& c, int age) {
    auto_ptr<DBClientCursor> cursor = c.query("tutorial.persons", QUERY( "age" << age ).sort("name") );
    while( cursor->more() ) {
        BSONObj p = cursor->next();
        cout << p.getStringField("name") << endl;
    }
}

void run() {
    DBClientConnection c;
    c.connect("localhost"); //"192.168.58.1");
    cout << "connected ok" << endl;
    BSONObj p = BSON( "name" << "Joe" << "age" << 33 );
    c.insert("tutorial.persons", p);
    p = BSON( "name" << "Jane" << "age" << 40 );
    c.insert("tutorial.persons", p);
    p = BSON( "name" << "Abe" << "age" << 33 );
    c.insert("tutorial.persons", p);
    p = BSON( "name" << "Methuselah" << "age" << BSONNULL);
    c.insert("tutorial.persons", p);
    p = BSON( "name" << "Samantha" << "age" << 21 << "city" << "Los Angeles" << "state" << "CA" );
    c.insert("tutorial.persons", p);

    c.ensureIndex("tutorial.persons", fromjson("{age:1}"));

    cout << "count:" << c.count("tutorial.persons") << endl;

    auto_ptr<DBClientCursor> cursor = c.query("tutorial.persons", BSONObj());
    while( cursor->more() ) {
        cout << cursor->next().toString() << endl;
    }

    cout << "\nprintifage:\n";
    printIfAge(c, 33);
}

int main() {
    try {
        run();
    }
    catch( DBException &e ) {
        cout << "caught " << e.what() << endl;
    }
    return 0;
}
