// @file oplogreader.cpp

/**
*    Copyright (C) 2008 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "pch.h"
#include "mongo/db/oplogreader.h"

namespace mongo {
	const BSONObj reverseNaturalObj = BSON( "$natural" << -1 );

	OplogReader::OplogReader( bool doHandshake ) : 
        _doHandshake( doHandshake ) { 
        
        _tailingQueryOptions = QueryOption_SlaveOk;
        _tailingQueryOptions |= QueryOption_CursorTailable | QueryOption_OplogReplay;
        
        /* TODO: slaveOk maybe shouldn't use? */
        _tailingQueryOptions |= QueryOption_AwaitData;

        //readersCreatedStats.increment();
    }

    bool OplogReader::commonConnect(const string& hostName) {
        if( conn() == 0 ) {
            _conn = shared_ptr<DBClientConnection>(new DBClientConnection(false,
                                                                          0,
                                                                          30 /* tcp timeout */));
            string errmsg;
            if ( !_conn->connect(hostName.c_str(), errmsg) /*||
                 (!noauth && !replAuthenticate(_conn.get(), true))*/ ) {
                resetConnection();
                log() << "repl: " << errmsg << endl;
                return false;
            }
        }
        return true;
    }
    
    bool OplogReader::connect(const std::string& hostName) {
        if (conn() != 0) {
            return true;
        }

        if ( ! commonConnect(hostName) ) {
            return false;
        }
        
        
        if ( _doHandshake /*&& ! replHandshake(_conn.get() )*/ ) {
            return false;
        }

        return true;
    }

    bool OplogReader::connect(const BSONObj& rid, const int from, const string& to) {
        if (conn() != 0) {
            return true;
        }
        if (commonConnect(to)) {
            log() << "handshake between " << from << " and " << to << endl;
            return passthroughHandshake(rid, from);
        }
        return false;
    }

    bool OplogReader::passthroughHandshake(const BSONObj& rid, const int nextOnChainId) {
        /*BSONObjBuilder cmd;
        cmd.appendAs(rid["_id"], "handshake");
        if (theReplSet) {
            const Member* chainedMember = theReplSet->findById(nextOnChainId);
            if (chainedMember != NULL) {
                cmd.append("config", chainedMember->config().asBson());
            }
        }
        cmd.append("member", nextOnChainId);

        BSONObj res;
        return conn()->runCommand("admin", cmd.obj(), res);*/
		return true;
    }

    void OplogReader::tailingQuery(const char *ns, const BSONObj& query, const BSONObj* fields ) {
        verify( !haveCursor() );
        LOG(2) << "repl: " << ns << ".find(" << query.toString() << ')' << endl;
        cursor.reset( _conn->query( ns, query, 0, 0, fields, _tailingQueryOptions ).release() );
    }

    void OplogReader::tailingQueryGTE(const char *ns, OpTime optime, const BSONObj* fields ) {
        BSONObjBuilder gte;
        gte.appendTimestamp("$gte", optime.asDate());
        BSONObjBuilder query;
        query.append("ts", gte.done());
        tailingQuery(ns, query.done(), fields);
    }

} // namespace mongo
