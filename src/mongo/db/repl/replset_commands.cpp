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

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/oplogreader.h"
#include "mongo/db/repl/rs_optime.h"
#include "../cmdline.h"
#include "../commands.h"
#include "../repl.h"
#include "health.h"
#include "rs.h"
#include "rs_config.h"
#include "../dbwebserver.h"
#include "../../util/mongoutils/html.h"
#include "../repl_block.h"
#include "connections.h"
#include "../../client/connpool.h"

using namespace bson;

namespace mongo {

    void checkMembersUpForConfigChange(const ReplSetConfig& cfg, BSONObjBuilder& result, bool initial);

    /* commands in other files:
         replSetHeartbeat - health.cpp
         replSetInitiate  - rs_mod.cpp
    */

    bool replSetBlind = false;
    unsigned replSetForceInitialSyncFailure = 0;

    // Testing only, enabled via command-line.
    class CmdReplSetTest : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "Just for regression tests.\n";
        }
        // No auth needed because it only works when enabled via command line.
        virtual bool requiresAuth() { return false; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}
        CmdReplSetTest() : ReplSetCommand("replSetTest") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            log() << "replSet replSetTest command received: " << cmdObj.toString() << rsLog;

            if( cmdObj.hasElement("forceInitialSyncFailure") ) {
                replSetForceInitialSyncFailure = (unsigned) cmdObj["forceInitialSyncFailure"].Number();
                return true;
            }

            if( !check(errmsg, result) )
                return false;

            if( cmdObj.hasElement("blind") ) {
                replSetBlind = cmdObj.getBoolField("blind");
                return true;
            }

            if (cmdObj.hasElement("sethbmsg")) {
                replset::sethbmsg(cmdObj["sethbmsg"].String());
                return true;
            }

            return false;
        }
    };
    MONGO_INITIALIZER(RegisterReplSetTestCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdReplSetTest();
        }
        return Status::OK();
    }

    /** get rollback id.  used to check if a rollback happened during some interval of time.
        as consumed, the rollback id is not in any particular order, it simply changes on each rollback.
        @see incRBID()
    */
    class CmdReplSetGetRBID : public ReplSetCommand {
    public:
        /* todo: ideally this should only change on rollbacks NOT on mongod restarts also. fix... */
        int rbid;
        virtual void help( stringstream &help ) const {
            help << "internal";
        }
        CmdReplSetGetRBID() : ReplSetCommand("replSetGetRBID") {
            // this is ok but micros or combo with some rand() and/or 64 bits might be better --
            // imagine a restart and a clock correction simultaneously (very unlikely but possible...)
            rbid = (int) curTimeMillis64();
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetGetRBID);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            result.append("rbid",rbid);
            return true;
        }
    } cmdReplSetRBID;

    /** we increment the rollback id on every rollback event. */
    void incRBID() {
        cmdReplSetRBID.rbid++;
    }

    /** helper to get rollback id from another server. */
    int getRBID(DBClientConnection *c) {
        bo info;
        c->simpleCommand("admin", &info, "replSetGetRBID");
        return info["rbid"].numberInt();
    }

    class CmdReplSetGetStatus : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "Report status of a replica set from the POV of this server\n";
            help << "{ replSetGetStatus : 1 }";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetGetStatus);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetGetStatus() : ReplSetCommand("replSetGetStatus", true) { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if ( cmdObj["forShell"].trueValue() )
                lastError.disableForCommand();

            if( !check(errmsg, result) )
                return false;
            theReplSet->summarizeStatus(result);
            return true;
        }
    } cmdReplSetGetStatus;

    class CmdReplSetReconfig : public ReplSetCommand {
        RWLock mutex; /* we don't need rw but we wanted try capability. :-( */
    public:
        virtual void help( stringstream &help ) const {
            help << "Adjust configuration of a replica set\n";
            help << "{ replSetReconfig : config_object }";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetReconfig);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetReconfig() : ReplSetCommand("replSetReconfig"), mutex("rsreconfig") { }
        virtual bool run(const string& a, BSONObj& b, int e, string& errmsg, BSONObjBuilder& c, bool d) {
            try {
                rwlock_try_write lk(mutex);
                return _run(a,b,e,errmsg,c,d);
            }
            catch(rwlock_try_write::exception&) { }
            errmsg = "a replSetReconfig is already in progress";
            return false;
        }
    private:
        bool _run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( cmdObj["replSetReconfig"].type() != Object ) {
                errmsg = "no configuration specified";
                return false;
            }

            bool force = cmdObj.hasField("force") && cmdObj["force"].trueValue();
            if( force && !theReplSet ) {
                replSettings.reconfig = cmdObj["replSetReconfig"].Obj().getOwned();
                result.append("msg", "will try this config momentarily, try running rs.conf() again in a few seconds");
                return true;
            }

            if ( !check(errmsg, result) ) {
                return false;
            }

            if( !force && !theReplSet->box.getState().primary() ) {
                errmsg = "replSetReconfig command must be sent to the current replica set primary.";
                return false;
            }

            {
                // just make sure we can get a write lock before doing anything else.  we'll reacquire one
                // later.  of course it could be stuck then, but this check lowers the risk if weird things
                // are up - we probably don't want a change to apply 30 minutes after the initial attempt.
                time_t t = time(0);
                Lock::GlobalWrite lk;
                if( time(0)-t > 20 ) {
                    errmsg = "took a long time to get write lock, so not initiating.  Initiate when server less busy?";
                    return false;
                }
            }

            try {
                scoped_ptr<ReplSetConfig> newConfig
                        (ReplSetConfig::make(cmdObj["replSetReconfig"].Obj(), force));

                log() << "replSet replSetReconfig config object parses ok, " <<
                        newConfig->members.size() << " members specified" << rsLog;

                if( !ReplSetConfig::legalChange(theReplSet->getConfig(), *newConfig, errmsg) ) {
                    return false;
                }

                checkMembersUpForConfigChange(*newConfig, result, false);

                log() << "replSet replSetReconfig [2]" << rsLog;

                theReplSet->haveNewConfig(*newConfig, true);
                ReplSet::startupStatusMsg.set("replSetReconfig'd");
            }
            catch( DBException& e ) {
                log() << "replSet replSetReconfig exception: " << e.what() << rsLog;
                throw;
            }
            catch( string& se ) {
                log() << "replSet reconfig exception: " << se << rsLog;
                errmsg = se;
                return false;
            }

            resetSlaveCache();
            return true;
        }
    } cmdReplSetReconfig;

    class CmdReplSetFreeze : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetFreeze : <seconds> }";
            help << "'freeze' state of member to the extent we can do that.  What this really means is that\n";
            help << "this node will not attempt to become primary until the time period specified expires.\n";
            help << "You can call again with {replSetFreeze:0} to unfreeze sooner.\n";
            help << "A process restart unfreezes the member also.\n";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetFreeze);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetFreeze() : ReplSetCommand("replSetFreeze") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            int secs = (int) cmdObj.firstElement().numberInt();
            if( theReplSet->freeze(secs) ) {
                if( secs == 0 )
                    result.append("info","unfreezing");
            }
            if( secs == 1 )
                result.append("warning", "you really want to freeze for only 1 second?");
            return true;
        }
    } cmdReplSetFreeze;

    class CmdGetIdentifier : public ReplSetCommand {
    public:
        CmdGetIdentifier() : ReplSetCommand("getIdentifier") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replGetIdentifier);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
			cout << "[MYCODE] Get Identifier Command called" << endl;
            if( !check(errmsg, result) )
                return false;

			vector<ReplSetConfig::MemberCfg> configMembers = theReplSet->config().members;
			vector<string> hosts;
			vector<int> ids;
			for( vector<ReplSetConfig::MemberCfg>::const_iterator i = configMembers.begin(); i != configMembers.end(); i++ ) {
				cout << "[MYCODE] Host:" << i->h.toString() << " ID:" << i->_id << endl; 
				hosts.push_back(i->h.toString());
				ids.push_back(i->_id);
			}

			result.append("hosts",hosts);
			result.append("id",ids);

            return true;
        }
    } cmdGetIdentifier;

    class CmdReplSetLeader : public ReplSetCommand {
    public:
        CmdReplSetLeader() : ReplSetCommand("replSetLeader") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetLeader);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
			cout << "[MYCODE] Replica Set Leader Command called" << endl;
            if( !check(errmsg, result) )
                return false;

			try {
			    theReplSet->elect.electSelf();
			}
			catch(RetryAfterSleepException&) {
			    /* we want to process new inbounds before trying this again.  so we just put a checkNewstate in the queue for eval later. */
				cout << "[MYCODE] Retry after sleep exception" << endl;
			}
			catch(...) {
			    cout << "[MYCODE] replSet error unexpected assertion in rs manager" << rsLog << endl;
			}
            return true;
        }
    } cmdReplSetLeader;

    class CmdReplSetRemove : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetRemove : <host> }";
            help << "'remove' of member from the replica set. For primary it steps down first\n";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetRemove);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetRemove() : ReplSetCommand("replSetRemove") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
			/*if ( cmdObj["replSetRemove"] != String ) {
				errmsg = "no hostname specified";
				return false;
			}*/
            if( !check(errmsg, result) )
                return false;

			string host = cmdObj["replSetRemove"].String();
			BSONObj config = theReplSet->getConfig().asBson().getOwned();
			cout << "[MYCODE] ReplSetRemove CMDOBJ:" << cmdObj.toString() << endl;
			cout << "[MYCODE] ReplSetRemove CONFIGPRINT:" << config.toString() << "\n";

			string id = config["_id"].String();
			cout << "[MYCODE] ReplSetRemove ID:" << config << "\n";
			int version = config["version"].Int();

			vector<ReplSetConfig::MemberCfg> configMembers = theReplSet->config().members;
			string myid = theReplSet->config()._id;
			int max = 0;
			for( vector<ReplSetConfig::MemberCfg>::const_iterator i = configMembers.begin(); i != configMembers.end(); i++ ) { 
			    if (i->h.isSelf()) {
			        continue;
			    }
	
			    BSONObj res;
			    {
			        bool ok = false;
			        try {
			            int theirVersion = -1000;
			            ok = requestHeartbeat(myid, "", i->h.toString(), res, -1, theirVersion, false); 
			            if( max >= theirVersion ) { 
							max = theirVersion;
						}
					}
					catch(DBException& e) { 
						log() << "replSet cmufcc requestHeartbeat " << i->h.toString() << " : " << e.toString() << rsLog; 
               		}
               		catch(...) { 
                   		log() << "replSet cmufcc error exception in requestHeartbeat?" << rsLog; 
               		}
				}
			}

			version = max > version ? max : version;
			version++;

			vector<BSONElement> members = config["members"].Array();
			BSONObjBuilder update;
			update.append("_id", id);
			update.append("version", version);
			BSONArrayBuilder newMember(update.subarrayStart("members"));
			for (vector<BSONElement>::iterator it = members.begin(); it != members.end(); it++)
			{
				BSONObj hostObj = (*it).Obj();
				if (!host.compare(hostObj["host"].String()))
				{
					//result.append("id",hostObj["_id"].Int());
					//cout << "[MYCODE] Removed Host " << hostObj["host"].String() << " ID:" << result.done().toString() << endl;
					continue;
				}
				newMember.append(*it);
			}

			newMember.done();
			BSONObj updateObj = update.done();
			printf("[MYCODE] ReplSetRemove UPDATE: %s\n", updateObj.toString().c_str());

			BSONObj info;

			try
			{
                scoped_ptr<ReplSetConfig> newConfig
                        (ReplSetConfig::make(updateObj, true));

                log() << "replSet replSetReconfig config object parses ok, " <<
                        newConfig->members.size() << " members specified" << rsLog;

                if( !ReplSetConfig::legalChange(theReplSet->getConfig(), *newConfig, errmsg) ) {
                    return false;
                }

                checkMembersUpForConfigChange(*newConfig, result, false);

                log() << "replSet replSetReconfig [2]" << rsLog;

                theReplSet->haveNewConfig(*newConfig, true);
                ReplSet::startupStatusMsg.set("replSetReconfig'd");
			}
			catch(DBException &e) {
				cout << "[MYCODE] ReplSetRemove Trying to remove the host" << host << "threw exception: " << e.toString() << endl;
			}

			return true;
        }
    } cmdReplSetRemove;

    class CmdReplSetAdd : public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ {replSetAdd : <host>}, {primary: true} }";
            help << "'add' member to the replica set. If primary is true then add as primary\n";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetAdd);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetAdd() : ReplSetCommand("replSetAdd") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
			/*if ( cmdObj["replSetAdd"] != String ) {
				errmsg = "no hostname specified";
				return false;
			}*/
			if( !check(errmsg, result) )
                return false;

			cout << "[MYCODE] ReplSetAdd CMDOBJ:" << cmdObj.toString() << endl;
			string addedHost = cmdObj["replSetAdd"].String();
			bool wantPrimary = cmdObj["primary"].Bool();
			int addedHostID = cmdObj["id"].Int();

			BSONObj config = theReplSet->getConfig().asBson().getOwned();
			cout << "[MYCODE] ReplSetAdd CONFIGPRINT:" << config.toString() << "\n";

			string id = config["_id"].String();
			int version = config["version"].Int();
			vector<ReplSetConfig::MemberCfg> configMembers = theReplSet->config().members;
			string myid = theReplSet->config()._id;
			int max = 0;
			for( vector<ReplSetConfig::MemberCfg>::const_iterator i = configMembers.begin(); i != configMembers.end(); i++ ) { 
			    // we know we're up 
			    if (i->h.isSelf()) {
			        continue;
			    }
	
			    BSONObj res;
			    {
			        bool ok = false;
			        try {
			            int theirVersion = -1000;
			            ok = requestHeartbeat(myid, "", i->h.toString(), res, -1, theirVersion, false); 
			            if( max >= theirVersion ) { 
							max = theirVersion;
						}
					}
					catch(DBException& e) { 
						log() << "replSet cmufcc requestHeartbeat " << i->h.toString() << " : " << e.toString() << rsLog; 
               		}
               		catch(...) { 
                   		log() << "replSet cmufcc error exception in requestHeartbeat?" << rsLog; 
               		}
				}
			}

			version = max > version ? max : version;
			version++;

			vector<BSONElement> members = config["members"].Array();
			BSONObjBuilder update;
			update.append("_id", id);
			update.append("version", version);
			BSONArrayBuilder newMember(update.subarrayStart("members"));
			double maxPr = 1;
			for (vector<BSONElement>::iterator it = members.begin(); it != members.end(); it++)
			{
				BSONObj hostObj = (*it).Obj();
				cout << "[MYCODE] ReplSetAdd MEMBER:" << hostObj.toString() << endl;
				newMember.append(*it);

				if (hostObj["priority"].ok() && maxPr < hostObj["priority"].Double())
					maxPr = hostObj["priority"].Double();
			}

			if (wantPrimary)
				newMember.append(BSON("host" << addedHost << "_id" << addedHostID << "priority" << maxPr + 1));
			else
				newMember.append(BSON("host" << addedHost << "_id" << addedHostID));

			newMember.done();
			BSONObj updateObj = update.done();
			printf("[MYCODE] ReplSetAdd UPDATE: %s\n", updateObj.toString().c_str());

			try
			{
                scoped_ptr<ReplSetConfig> newConfig
                        (ReplSetConfig::make(updateObj, true));

                log() << "replSet replSetReconfig config object parses ok, " <<
                        newConfig->members.size() << " members specified" << rsLog;

                if( !ReplSetConfig::legalChange(theReplSet->getConfig(), *newConfig, errmsg) ) {
                    return false;
                }

                checkMembersUpForConfigChange(*newConfig, result, false);

                log() << "replSet replSetReconfig [2]" << rsLog;

                theReplSet->haveNewConfig(*newConfig, true);
                ReplSet::startupStatusMsg.set("replSetReconfig'd");	
			}
			catch(DBException &e) {
				cout << "[MYCODE] ReplSetRemove Trying to remove the host" << addedHost << "threw exception: " << e.toString() << endl;
			}

			cout << "[MYCODE] Replica Set Current Version:" << theReplSet->config().version << " Local Computed Version:" << version << endl;

			BSONObj cmd = BSON("replSetReconfig" << updateObj << "force" << true);

			BSONObj info;
			for( vector<ReplSetConfig::MemberCfg>::const_iterator i = configMembers.begin(); i != configMembers.end(); i++ ) { 
			
				string hostStr = i->h.toString();
				cout << "[MYCODE] Sending replSetReconfig to Host:" << hostStr << endl;

				if (i->h.isSelf())
					continue;
				
				scoped_ptr<ScopedDbConnection> conn(
					ScopedDbConnection::getInternalScopedDbConnection(hostStr));

				try
				{
					if (!conn->get()->runCommand("admin", cmd, info, 0))
					{
						cout << "[MYCODE] ReplSetAdd failed to reconfigure the replica set\n";
					}

					string errmsg = conn->get()->getLastError();
					cout << "[MYCODE] ReplSetAdd Error:" << errmsg << endl;
				}
				catch(DBException &e) {
					cout << "[MYCODE] ReplSetAdd Trying to add the host " << addedHost << " threw exception: " << e.toString() << endl;
				}

				conn->done();
			}
           
			scoped_ptr<ScopedDbConnection> hostConn(
				ScopedDbConnection::getInternalScopedDbConnection(addedHost));
			try
			{
				if (!hostConn->get()->runCommand("admin", cmd, info, 0))
				{
					cout << "[MYCODE] ReplSetAdd failed to reconfigure the replica set\n";
				}

				string errmsg = hostConn->get()->getLastError();
				cout << "[MYCODE] ReplSetAdd Error:" << errmsg << endl;
				if (wantPrimary)
				{
		           	theReplSet->stepDown(120);
					hostConn->get()->runCommand("admin", BSON("replSetLeader" << 1 << "priority" << maxPr + 1), info, 0);
				}
			}
			catch(DBException &e) {
				cout << "[MYCODE] ReplSetAdd Trying to add the host " << addedHost << " threw exception: " << e.toString() << endl;
			}
			hostConn->done();

            return true;
        }
    } cmdReplSetAdd;


    class CmdReplayOplog : public ReplSetCommand {
    private:
        OplogReader oplogReader;

    public:
        virtual void help( stringstream &help ) const {
            help << "{ {replayOplog : <oplogParams>} }";
            help << "replay the oplog\n";
            help << "\nhttp://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replayOplog);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplayOplog() : ReplSetCommand("replayOplog") { }
        
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            //check if there is already an error message, if there is return immediately
            if( !check(errmsg, result) )
                return false;

            //the success variable
            bool success = true;

            printLogID();
            cout<<"====running replayOplog command====="<<endl;
            //extract the arguments package
            BSONObj oplogParams = cmdObj["replayOplog"].Obj().getOwned();

            //declare the arguments we want
            string ns;                                          //namespace
            OpTime startTime;                                   //time from which oplog needs to be replayed        
            string primary;                               	    //the primary to connect to (for oplog details)
            int shardID;                                        //the id for this shard
            BSONObj proposedKey;                                //the proposed key
            BSONObj globalMin;                                  //global min
            BSONObj globalMax;                                  //global max
            int numChunks;                                      //number of chunks
            vector<BSONObj> splitPoints;                        //split points
            vector<int> assignments;                            //the new assignments for chunks
            vector<string> removedReplicas;                     //the other removed replicas                            
            
            //do some checks to see we have all the info we require
            //also extract the arguments in the same call
            if( !checkAndExtractArgs(oplogParams, errmsg,
                                        ns, startTime, primary, shardID, numChunks, 
                                        proposedKey, globalMin, globalMax,
                                        splitPoints, assignments,removedReplicas)) {
                success = false;
            } else {

                printExtractedArgs(ns, startTime, primary, shardID, numChunks, 
                                        proposedKey, globalMin, globalMax,
                                        splitPoints, assignments,removedReplicas);

                //replay the oplog
                success =  replayOplog(errmsg, ns, startTime, primary, shardID, numChunks, 
                                        proposedKey, globalMin, globalMax,
                                        splitPoints, assignments,removedReplicas);
            }
            
            if (success) {
                printLogID();
                cout<<"Replay succeeded !" << endl;
            } else {
                printLogID();
                cout<<"Replay failed! Error: " << errmsg << endl;
            }

            return success;
        }

        bool replayOplog(string& errmsg, string ns, OpTime startTime, string primary, int shardID, int numChunks,
                                    BSONObj proposedKey, BSONObj globalMin, BSONObj globalMax, 
                                    vector<BSONObj> splitPoints, vector<int> assignments, vector<string> removedReplicas) {

            //printLogID();
            //cout<<"====running replayOplog====="<<endl;
            int iterations = 10;            //number of times to iterate
            bool done = false;              //are we done yet?
            BSONObj prevlastOp = BSONObj(); //operation checking

            //attempt to replay oplog 'iterations' times
            for(int i = 0; i < iterations; i++) {
                printLogID();
                cout<<"Executing iteration of replayOnce: " << i <<endl;
                if( replayOnce(errmsg, done, prevlastOp, startTime, ns, primary, shardID, numChunks,
                                    proposedKey, globalMin, globalMax,
                                    splitPoints, assignments, removedReplicas) 
                                && done) {
                    //if replay once succeded and done is true, there were no more ops to replay
                    printLogID();
                    cout<<"Done in iterations: " << i <<endl;
                    return true;
                }

                //printLogID();
                //cout<<"Prev op in replayOplog: "<<prevlastOp.toString()<<endl;

                //if not, startTime is automatically the new start time and the next iteration of replayOnce will receive it.

                //check if there were any errors
                if(errmsg != "" ) {
                    //there was an error
                    return false;
                }
            }          
            
            //no success after 10 iterations :(
            errmsg="Replaying oplog (multiple attempts) did not succeed";
            return false;
        }

        void printLogID() {
            cout<<"[MYCODE_HOLLA] ";
        }

        void printExtractedArgs(string& ns, OpTime& startTime, string& primary, int& shardID, int& numChunks,
                                    BSONObj& proposedKey, BSONObj& globalMin, BSONObj& globalMax,
                                    vector<BSONObj>& splitPoints, vector<int>& assignments, vector<string>& removedReplicas) {
            printLogID();
            cout<<"Namespace is: "<<ns<<endl;
            printLogID();
            cout<<"Start time is: "<<startTime.toString()<<endl;
            printLogID();
            cout<<"Primary is: "<<primary<<endl;
            printLogID();
            cout<<"Shard id is: "<<shardID<<endl;
            printLogID();
            cout<<"Num chunks: "<<numChunks<<endl;
            printLogID();
            cout<<"Proposed key: "<<proposedKey.toString()<<endl;
            printLogID();
            cout<<"Global min: "<<globalMin.toString()<<endl;
            printLogID();
            cout<<"Global max: "<<globalMax.toString()<<endl;
            printLogID();
            cout<<"Split points: ";
            for(int i = 0; i < (int)splitPoints.size(); i++) {
                cout<<splitPoints[i].toString()<<" | ";
            }
            cout<<endl;
            printLogID();
            cout<<"Assignments: ";
            for(int i = 0; i < (int)assignments.size(); i++) {
                cout<<assignments[i]<<" | ";
            }
            cout<<endl;
            printLogID();
            cout<<"Removed replicas: ";
            for(int i = 0; i < (int)removedReplicas.size(); i++) {
                cout<<removedReplicas[i]<<" | ";
            }
            cout<<endl;
        }

        bool checkAndExtractArgs(BSONObj oplogParams, string& errmsg,
                                    string& ns, OpTime& startTime, string& primary, int& shardID, int& numChunks, 
                                    BSONObj& proposedKey, BSONObj& globalMin, BSONObj& globalMax,
                                    vector<BSONObj>& splitPoints, vector<int>& assignments, vector<string>& removedReplicas) {
            //printLogID();
            //cout<<"====Arg extraction====="<<endl;
            //namespace
            ns = oplogParams["ns"].String();
            if ( ns.size() == 0 ) {
                errmsg = "no ns";
                return false;
            } else {
                const NamespaceString nsStr( ns );
                if ( !nsStr.isValid() ){
                    errmsg = str::stream() << "bad ns[" << ns << "]";
                    return false;
                }
            }

            //printLogID();
            //cout<<"namespace done"<<endl;

            //start time
            startTime = oplogParams["startTime"]._opTime();
            if (startTime.isNull()) {
                errmsg = "no start time";
                return false;
            }

            //printLogID();
            //cout<<"start time done"<<endl;

            //primary
            primary = oplogParams["primary"].String();
            if ( primary.size() == 0 ) {
                errmsg = "no primary provided";
                return false;
            }

            //printLogID();
            //cout<<"primary done"<<endl;

            //shard id
            shardID = oplogParams["shardID"].Int();

            //printLogID();
            //cout<<"shard id done"<<endl;

            //number of chunks
            numChunks = oplogParams["numChunks"].Int();

            //printLogID();
            //cout<<"numChunks done"<<endl;

            //proposed key
            proposedKey = oplogParams["proposedKey"].Obj();
            if ( proposedKey.isEmpty() ) {
                errmsg = "no shard key";
                return false;
            }

            //printLogID();
            //cout<<"proposedKey done"<<endl;

            //global min
            globalMin = oplogParams["globalMin"].Obj();
            if ( globalMin.isEmpty() ) {
                errmsg = "no global min";
                return false;
            }

            //printLogID();
            //cout<<"globalMin done"<<endl;

            //global max
            globalMax = oplogParams["globalMax"].Obj();
            if ( globalMax.isEmpty() ) {
                errmsg = "no global max";
                return false;
            }

            //printLogID();
            //cout<<"globalMax done"<<endl;


            //split points
            vector<BSONElement> splitPointsRaw = oplogParams["splitPoints"].Array();
            for (vector<BSONElement>::iterator point = splitPointsRaw.begin(); point != splitPointsRaw.end(); point++) {
                splitPoints.push_back((*point).Obj());
            }

            //printLogID();
            //cout<<"splitPoints done"<<endl;


            //assignments
            vector<BSONElement> assignmentsRaw = oplogParams["assignments"].Array();
            for (vector<BSONElement>::iterator assignment = assignmentsRaw.begin(); assignment != assignmentsRaw.end(); assignment++){
               assignments.push_back((*assignment).Int());
            }

            //printLogID();
            //cout<<"assignments done"<<endl;

            //removed replicas
            vector<BSONElement> removedReplicasRaw = oplogParams["removedReplicas"].Array();
            for (vector<BSONElement>::iterator removedReplica = removedReplicasRaw.begin(); removedReplica != removedReplicasRaw.end(); removedReplica++){
               removedReplicas.push_back((*removedReplica).String());
            }

            //printLogID();
            //cout<<"removedReplicas done"<<endl;
            return true;
        }

        bool replayOnce(string& errmsg, bool& done, BSONObj& prevlastOp, OpTime& startTime, string ns, string primary, int shardID, int numChunks,
                                    BSONObj proposedKey, BSONObj globalMin, BSONObj globalMax,
                                    vector<BSONObj> splitPoints, vector<int> assignments, vector<string> removedReplicas) {
           
            //printLogID();
            //cout<<"====running replayOnce====="<<endl;
            bool success = false;           //the success variable

            //get all the ops that need to be replayed
            vector<BSONObj> opsToReplay;
            BSONObj lastOp = BSONObj();
            success = getOpsToReplay(errmsg, lastOp, startTime, ns, primary, opsToReplay);

            if(success) {
                printLogID();
                cout<<"Operations to replay: "<<opsToReplay.size()<<endl;
                //check if any more ops to replay
                //printLogID();
                //cout<<"Prev op before checking last op: "<<prevlastOp.toString()<<endl;
                //printLogID();
                //cout<<"Last op: "<<lastOp.toString()<<endl;
                if (prevlastOp == lastOp) { //TODO GOPAL: a comparison on the 'h' field should be enough
                    //no more ops to replay. say we are done!
                    done = true;
                } else {
                    //replay 'em!
                    success = replayOps(errmsg, ns, opsToReplay, numChunks, 
                                            proposedKey, globalMin, globalMax,
                                            splitPoints, assignments, removedReplicas);
                }
            }

            prevlastOp = lastOp.getOwned();
            //printLogID();
            //cout<<"Prev op after assignment in replayOnce: "<<prevlastOp.toString()<<endl;
            return success;
        }

        bool getOpsToReplay(string& errmsg, BSONObj& lastOp, OpTime& startTime, string ns, string primary, vector<BSONObj>& opsToReplay) {
            BSONObj info;       //return info
            
            printLogID();
            cout<<"==== Getting ops to replay===="<<endl;
            //TODO GOPAL: Can we connect one time and leave it?
            //connect to primary
            oplogReader.connect(primary);

            //make a query
            oplogReader.tailingQueryGTE(rsoplog, startTime);

            //gather up thr results
            BSONObj queryResult;
            while (oplogReader.more())
            {
                queryResult = oplogReader.next();
                //set new starttime
                startTime = queryResult["ts"]._opTime();
                //printLogID();
                //cout<<"Oplog operation:  " <<queryResult.toString()<<endl;
                //add it to all ops
                opsToReplay.push_back(queryResult);
                //set it as last op
                lastOp = queryResult.getOwned();
            }
            
            //TODO GOPAL: Can we connect one time and leave it?
            //snap this connection
            oplogReader.resetConnection();
            return true;
        }

        bool replayOps(string& errmsg, string nsOrig, vector<BSONObj> opsToReplay, int numChunks, 
                            BSONObj proposedKey, BSONObj globalMin, BSONObj globalMax, 
                            vector<BSONObj> splitPoints, vector<int> assignments, vector<string> removedReplicas) {
            
            bool success = true;        //success variable
            
            cout<<"==== Replaying ops===="<<endl;
            //TODO GOPAL: Handle error conditions
            //iterate through each op, replaying it
            for (int i = 0; i < (int)opsToReplay.size(); i++) {
                //get the op to replay
                BSONObj opToReplay = opsToReplay[i];

                printLogID();
                cout<<"Currently replaying oplog entry: " <<opToReplay.toString()<<endl;

                //get the operation
                string op = opToReplay["op"].String();
                //printLogID();
                //cout<<"op is: " <<op<<endl;
                //do actions only if op is insert, update or delete
                if( op == "i" || op == "u" || op == "d") { //TOASK GOPAL: Why? What are the other kind of operations?
                    //get the namespace
                    string ns = opToReplay["ns"].String();
                    //printLogID();
                    //cout<<"ns is: " <<ns<<endl;

                    //get the 'o' field
                    BSONObj o = opToReplay["o"].Obj();
                    //printLogID();
                    //cout<<"o is: " <<o.toString()<<endl;

                    //need to do processing only if it is the namespace we are interested in
                    if(ns == nsOrig) {
                        //get the value of the proposed key
                        //TOCHECK GOPAL: How do you do this the right way?
                        //1. do hasField
                        //2. What do you do if does not?
                        BSONObj value  = o.extractFields(proposedKey);
                        //printLogID();
                        //cout << "Value string : " << value.toString() << endl;

                        //attempt find which chunk this record will now go to
                        int chunkIndex = getChunkIndex(value, globalMin, globalMax, splitPoints, numChunks);

                        if (chunkIndex == -1) {
                            printLogID();
                            cout<<"Chunk index never assigned! Op: " << opToReplay.toString() << endl;
                            errmsg = "Chunk index not assigned for op: " + opToReplay.toString();
                            success = false;
                        } else if (assignments[chunkIndex] >= (int)removedReplicas.size()) {
                            printLogID();
                            cout<<"No replica found for assignment! Op: " << opToReplay.toString() << endl;
                            errmsg = "Replica to send to was not found for op ";
                            errmsg += "(assignment according to chunk index out of range): "; 
                            errmsg += opToReplay.toString();
                            success = false;
                        } else {
                            //printLogID();
                            //cout << "Operation " << o.toString() << " going to shard " << removedReplicas[assignments[chunkIndex]] << endl;
                            //TODO GOPAL: Optimize so that destMachine is not same as current machine. if it is, try local replay
                            if( !replayOp(ns, removedReplicas[assignments[chunkIndex]], op, o, opToReplay)) {
                                printLogID();
                                cout<<"Replay op error: "<<opToReplay.toString()<<endl;
                                success = false;
                                //TODO GOPAL: Handle error condition
                            } else {
                                printLogID();
                                cout<<"====replayOp done====="<<endl;
                            }
                        }
                    } else {
                        //TODO GOPAL: replay a non resharded operation
                        //obtain the object id
                        //see which shard the chunk belongs to
                        //replay on corresponding shard
                    }
                    
                }
            }    
            return success;
        }

        int getChunkIndex(BSONObj value, BSONObj globalMin, BSONObj globalMax, vector<BSONObj> splitPoints, int numChunks) {
            //printLogID();
            //cout<<"====getting chunk index====="<<endl;
            vector<BSONObj>::iterator point = splitPoints.begin();
            BSONObj prev;
            for (int i = 0; i < numChunks; i++)
            {
                BSONObj min = i > 0 ? prev : globalMin;
                BSONObj max = i == numChunks - 1 ? globalMax : *point;

                if ( min <= value && value < max)
                    //found it! mark the chunk index
                    return i;

                if (i < numChunks - 1)
                {
                    prev = *point;
                    point++;
                }
            }

            return -1;
        }

        bool replayOp(string ns, string destMachine, string op, BSONObj o, BSONObj opToReplay) {
            printLogID();
            cout<<"====running replayOp====="<<endl;
            bool success = true;
            //make the connection
            scoped_ptr<ScopedDbConnection> conn(
                ScopedDbConnection::getScopedDbConnection(
                    destMachine ) );

            //TODO GOPAL: do an error check if connections dont succeed
            
            //get source count
            int startCount = conn->get()->count(ns, BSONObj(), QueryOption_SlaveOk);

            //perform the operation
            if (op == "i"){
                conn->get()->insert(ns, o);
                
                
                string errmsg = conn->get()->getLastError();
                if(errmsg != "") {
                    printLogID();
                    cout<<"Insert ErrMsg: " << errmsg << " Op: " << opToReplay.toString() << endl;
                }
                
            } else if (op == "u") {
                //TODO GOPAL: handle a 'multi' field
                BSONObj query = opToReplay["o2"].Obj();
                //printLogID();
                //cout<<"o2 is: " <<query.toString()<<endl;
                bool upsert = opToReplay["b"].eoo() ? false : opToReplay["b"].Bool();
                //TODO GOPAL: see if this needs to be casted as query. Seems to work even without a cast.. for now
                conn->get()->update(ns, query, o, upsert);
                
                string errmsg = conn->get()->getLastError();
                if(errmsg != "") {
                    printLogID();
                    cout<<"Update ErrMsg: " << errmsg << " Op: " << opToReplay.toString() << endl;
                }       
            } else if (op == "d") {
                bool justOne = opToReplay["b"].eoo()? false : opToReplay["b"].Bool();
                conn->get()->remove(ns, o, justOne);
                
                
                string errmsg = conn->get()->getLastError();
                if(errmsg != "") {
                    printLogID();
                    cout<<"Remove ErrMsg: " << errmsg << " Op: " << opToReplay.toString() << endl;
                }
        
            } else {
                //TODO GOPAL: Handle error
                printLogID();
                cout<<"Unrecongnized op in replayOp! Op: " << opToReplay.toString() << endl;
                success = false;
            }

            int endCount = conn->get()->count(ns, BSONObj(), QueryOption_SlaveOk);
            //printLogID();
            //cout << "Start Count:" << startCount << " End Count:" << endCount << endl;

            conn->done();

            return success;
        }

    } cmdReplayOplog;

    class CmdReplSetStepDown: public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetStepDown : <seconds> }\n";
            help << "Step down as primary.  Will not try to reelect self for the specified time period (1 minute if no numeric secs value specified).\n";
            help << "(If another member with same priority takes over in the meantime, it will stay primary.)\n";
            help << "http://dochub.mongodb.org/core/replicasetcommands";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetStepDown);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetStepDown() : ReplSetCommand("replSetStepDown") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            if( !theReplSet->box.getState().primary() ) {
                errmsg = "not primary so can't step down";
                return false;
            }
			cout << "[MYCODE] ReplSetStepDown MYPRINT: replica set step down called" << endl;
            bool force = cmdObj.hasField("force") && cmdObj["force"].trueValue();

            // only step down if there is another node synced to within 10
            // seconds of this node
            if (!force) {
                long long int lastOp = (long long int)theReplSet->lastOpTimeWritten.getSecs();
                long long int closest = (long long int)theReplSet->lastOtherOpTime().getSecs();

                long long int diff = lastOp - closest;
                result.append("closest", closest);
                result.append("difference", diff);

                if (diff < 0) {
                    // not our problem, but we'll wait until thing settle down
                    errmsg = "someone is ahead of the primary?";
                    return false;
                }

                if (diff > 10) {
                    errmsg = "no secondaries within 10 seconds of my optime";
                    return false;
                }
            }

            int secs = (int) cmdObj.firstElement().numberInt();
            if( secs == 0 )
                secs = 60;
            return theReplSet->stepDown(secs);
        }
    } cmdReplSetStepDown;

    class CmdReplSetMaintenance: public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetMaintenance : bool }\n";
            help << "Enable or disable maintenance mode.";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetMaintenance);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetMaintenance() : ReplSetCommand("replSetMaintenance") { }
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;

            if (!theReplSet->setMaintenanceMode(cmdObj["replSetMaintenance"].trueValue())) {
                errmsg = "primaries can't modify maintenance mode";
                return false;
            }

            return true;
        }
    } cmdReplSetMaintenance;

    class CmdReplSetSyncFrom: public ReplSetCommand {
    public:
        virtual void help( stringstream &help ) const {
            help << "{ replSetSyncFrom : \"host:port\" }\n";
            help << "Change who this member is syncing from.";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetSyncFrom);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        CmdReplSetSyncFrom() : ReplSetCommand("replSetSyncFrom") { }
        virtual bool run(const string&, 
                         BSONObj& cmdObj, 
                         int, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) {
            if (!check(errmsg, result)) {
                return false;
            }
            string newTarget = cmdObj["replSetSyncFrom"].valuestrsafe();
            result.append("syncFromRequested", newTarget);
            return theReplSet->forceSyncFrom(newTarget, errmsg, result);
        }
    } cmdReplSetSyncFrom;

    using namespace bson;
    using namespace mongoutils::html;
    extern void fillRsLog(stringstream&);

    class ReplSetHandler : public DbWebHandler {
    public:
        ReplSetHandler() : DbWebHandler( "_replSet" , 1 , true ) {}

        virtual bool handles( const string& url ) const {
            return startsWith( url , "/_replSet" );
        }

        virtual void handle( const char *rq, const std::string& url, BSONObj params,
                             string& responseMsg, int& responseCode,
                             vector<string>& headers,  const SockAddr &from ) {

            if( url == "/_replSetOplog" ) {
                responseMsg = _replSetOplog(params);
            }
            else
                responseMsg = _replSet();
            responseCode = 200;
        }

        string _replSetOplog(bo parms) {
            int _id = (int) str::toUnsigned( parms["_id"].String() );

            stringstream s;
            string t = "Replication oplog";
            s << start(t);
            s << p(t);

            if( theReplSet == 0 ) {
                if( cmdLine._replSet.empty() )
                    s << p("Not using --replSet");
                else  {
                    s << p("Still starting up, or else set is not yet " + a("http://dochub.mongodb.org/core/replicasetconfiguration#ReplicaSetConfiguration-InitialSetup", "", "initiated")
                           + ".<br>" + ReplSet::startupStatusMsg.get());
                }
            }
            else {
                try {
                    theReplSet->getOplogDiagsAsHtml(_id, s);
                }
                catch(std::exception& e) {
                    s << "error querying oplog: " << e.what() << '\n';
                }
            }

            s << _end();
            return s.str();
        }

        /* /_replSet show replica set status in html format */
        string _replSet() {
            stringstream s;
            s << start("Replica Set Status " + prettyHostName());
            s << p( a("/", "back", "Home") + " | " +
                    a("/local/system.replset/?html=1", "", "View Replset Config") + " | " +
                    a("/replSetGetStatus?text=1", "", "replSetGetStatus") + " | " +
                    a("http://dochub.mongodb.org/core/replicasets", "", "Docs")
                  );

            if( theReplSet == 0 ) {
                if( cmdLine._replSet.empty() )
                    s << p("Not using --replSet");
                else  {
                    s << p("Still starting up, or else set is not yet " + a("http://dochub.mongodb.org/core/replicasetconfiguration#ReplicaSetConfiguration-InitialSetup", "", "initiated")
                           + ".<br>" + ReplSet::startupStatusMsg.get());
                }
            }
            else {
                try {
                    theReplSet->summarizeAsHtml(s);
                }
                catch(...) { s << "error summarizing replset status\n"; }
            }
            s << p("Recent replset log activity:");
            fillRsLog(s);
            s << _end();
            return s.str();
        }



    } replSetHandler;

}
