// d_migrate.cpp

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


/**
   these are commands that live in mongod
   mostly around shard management and checking
 */

#include "pch.h"

#include <algorithm>
#include <boost/thread/thread.hpp>
#include <map>
#include <string>
#include <vector>

#include "mongo/client/connpool.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/client/distlock.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/btreecursor.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/commands.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/dur.h"
#include "mongo/db/hasher.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/pagefault.h"
#include "mongo/db/queryoptimizer.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/repl/rs_config.h"
#include "mongo/db/pdfile.h"
#include "mongo/s/chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/config.h"
#include "mongo/s/d_logic.h"
#include "mongo/s/shard.h"
#include "mongo/s/type_chunk.h"
#include "mongo/util/elapsed_tracker.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/queue.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/startup_test.h"

using namespace std;

namespace mongo {

    bool findShardKeyIndexPattern_locked( const string& ns,
                                          const BSONObj& shardKeyPattern,
                                          BSONObj* indexPattern ) {
        verify( Lock::isLocked() );
        NamespaceDetails* nsd = nsdetails( ns );
        if ( !nsd )
            return false;
        const IndexDetails* idx = nsd->findIndexByPrefix( shardKeyPattern , true );  /* require single key */
        if ( !idx )
            return false;
        *indexPattern = idx->keyPattern().getOwned();
        return true;
    }

    bool findShardKeyIndexPattern_unlocked( const string& ns,
                                            const BSONObj& shardKeyPattern,
                                            BSONObj* indexPattern ) {
        Client::ReadContext context( ns );
        return findShardKeyIndexPattern_locked( ns, shardKeyPattern, indexPattern );
    }

    Tee* migrateLog = new RamLog( "migrate" );

    class MoveTimingHelper {
    public:
        MoveTimingHelper( const string& where , const string& ns , BSONObj min , BSONObj max , int total , string& cmdErrmsg )
            : _where( where ) , _ns( ns ) , _next( 0 ) , _total( total ) , _cmdErrmsg( cmdErrmsg ) {
            _nextNote = 0;
            _b.append( "min" , min );
            _b.append( "max" , max );
        }

        ~MoveTimingHelper() {
            // even if logChange doesn't throw, bson does
            // sigh
            try {
                if ( _next != _total ) {
                    note( "aborted" );
                }
                if ( _cmdErrmsg.size() ) {
                    note( _cmdErrmsg );
                    warning() << "got error doing chunk migrate: " << _cmdErrmsg << endl;
                }
                    
                configServer.logChange( (string)"moveChunk." + _where , _ns, _b.obj() );
            }
            catch ( const std::exception& e ) {
                warning() << "couldn't record timing for moveChunk '" << _where << "': " << e.what() << migrateLog;
            }
        }

        void done( int step ) {
            verify( step == ++_next );
            verify( step <= _total );

            stringstream ss;
            ss << "step" << step << " of " << _total;
            string s = ss.str();

            CurOp * op = cc().curop();
            if ( op )
                op->setMessage( s.c_str() );
            else
                warning() << "op is null in MoveTimingHelper::done" << migrateLog;

            _b.appendNumber( s , _t.millis() );
            _t.reset();

#if 0
            // debugging for memory leak?
            ProcessInfo pi;
            ss << " v:" << pi.getVirtualMemorySize()
               << " r:" << pi.getResidentSize();
            log() << ss.str() << migrateLog;
#endif
        }


        void note( const string& s ) {
            string field = "note";
            if ( _nextNote > 0 ) {
                StringBuilder buf;
                buf << "note" << _nextNote;
                field = buf.str();
            }
            _nextNote++;

            _b.append( field , s );
        }

    private:
        Timer _t;

        string _where;
        string _ns;

        int _next;
        int _total; // expected # of steps
        int _nextNote;

        string _cmdErrmsg;

        BSONObjBuilder _b;

    };

    struct OldDataCleanup {
        static AtomicUInt _numThreads; // how many threads are doing async cleanup

        bool secondaryThrottle;
        string ns;
        BSONObj min;
        BSONObj max;
        BSONObj shardKeyPattern;
        set<CursorId> initial;

        OldDataCleanup(){
            _numThreads++;
        }
        OldDataCleanup( const OldDataCleanup& other ) {
            secondaryThrottle = other.secondaryThrottle;
            ns = other.ns;
            min = other.min.getOwned();
            max = other.max.getOwned();
            shardKeyPattern = other.shardKeyPattern.getOwned();
            initial = other.initial;
            _numThreads++;
        }
        ~OldDataCleanup(){
            _numThreads--;
        }

        string toString() const {
            return str::stream() << ns << " from " << min << " -> " << max;
        }

        void doRemove() {
            ShardForceVersionOkModeBlock sf;
            {
                RemoveSaver rs("moveChunk",ns,"post-cleanup");

                log() << "moveChunk starting delete for: " << this->toString() << migrateLog;

                BSONObj indexKeyPattern;
                if ( !findShardKeyIndexPattern_unlocked( ns, shardKeyPattern, &indexKeyPattern ) ) {
                    warning() << "collection or index dropped before data could be cleaned" << endl;
                    return;
                }

                long long numDeleted =
                        Helpers::removeRange( ns,
                                              min,
                                              max,
                                              indexKeyPattern,
                                              false, /*maxInclusive*/
                                              secondaryThrottle,
                                              cmdLine.moveParanoia ? &rs : 0, /*callback*/
                                              true, /*fromMigrate*/
                                              true ); /*onlyRemoveOrphans*/ 

                log() << "moveChunk deleted " << numDeleted << " documents for "
                      << this->toString() << migrateLog;
            }

            ReplTime lastOpApplied = cc().getLastOp().asDate();
            Timer t;
            for ( int i=0; i<3600; i++ ) {
                if ( opReplicatedEnough( lastOpApplied , ( getSlaveCount() / 2 ) + 1 ) ) {
                    LOG(t.seconds() < 30 ? 1 : 0) << "moveChunk repl sync took " << t.seconds() << " seconds" << migrateLog;
                    return;
                }
                sleepsecs(1);
            }
            
            warning() << "moveChunk repl sync timed out after " << t.seconds() << " seconds" << migrateLog;
        }

    };

    AtomicUInt OldDataCleanup::_numThreads = 0;

    static const char * const cleanUpThreadName = "cleanupOldData";

    class ChunkCommandHelper : public Command {
    public:
        ChunkCommandHelper( const char * name )
            : Command( name ) {
        }

        virtual void help( stringstream& help ) const {
            help << "internal - should not be called directly";
        }
        virtual bool slaveOk() const { return false; }
        virtual bool adminOnly() const { return true; }
        virtual LockType locktype() const { return NONE; }

    };

    bool isInRange( const BSONObj& obj ,
                    const BSONObj& min ,
                    const BSONObj& max ,
                    const BSONObj& shardKeyPattern ) {
        ShardKeyPattern shardKey( shardKeyPattern );
        BSONObj k = shardKey.extractKey( obj );
        return k.woCompare( min ) >= 0 && k.woCompare( max ) < 0;
    }


    class MigrateFromStatus {
    public:

        MigrateFromStatus() : _mutex("MigrateFromStatus"),
			      _cleanupTickets(1) /* only one cleanup thread at once */ {
            _active = false;
            _inCriticalSection = false;
            _memoryUsed = 0;
        }

        /**
         * @return false if cannot start. One of the reason for not being able to
         *     start is there is already an existing migration in progress.
         */
        bool start( const std::string& ns ,
                    const BSONObj& min ,
                    const BSONObj& max ,
                    const BSONObj& shardKeyPattern ) {

            scoped_lock l(_mutex); // reads and writes _active

            if (_active) {
                return false;
            }

            verify( ! min.isEmpty() );
            verify( ! max.isEmpty() );
            verify( ns.size() );

            _ns = ns;
            _min = min;
            _max = max;
            _shardKeyPattern = shardKeyPattern;

            verify( _cloneLocs.size() == 0 );
            verify( _deleted.size() == 0 );
            verify( _reload.size() == 0 );
            verify( _memoryUsed == 0 );

            _active = true;
            return true;
        }

        void done() {
            log() << "MigrateFromStatus::done About to acquire global write lock to exit critical "
                    "section" << endl;
            Lock::GlobalWrite lk;
            log() << "MigrateFromStatus::done Global lock acquired" << endl;

            {
                scoped_spinlock lk( _trackerLocks );
                _deleted.clear();
                _reload.clear();
                _cloneLocs.clear();
            }
            _memoryUsed = 0;

            scoped_lock l(_mutex);
            _active = false;
            _inCriticalSection = false;
            _inCriticalSectionCV.notify_all();
        }

        void logOp( const char * opstr , const char * ns , const BSONObj& obj , BSONObj * patt ) {
            if ( ! _getActive() )
                return;

            if ( _ns != ns )
                return;

            // no need to log if this is not an insertion, an update, or an actual deletion
            // note: opstr 'db' isn't a deletion but a mention that a database exists (for replication
            // machinery mostly)
            char op = opstr[0];
            if ( op == 'n' || op =='c' || ( op == 'd' && opstr[1] == 'b' ) )
                return;

            BSONElement ide;
            if ( patt )
                ide = patt->getField( "_id" );
            else
                ide = obj["_id"];

            if ( ide.eoo() ) {
                warning() << "logOpForSharding got mod with no _id, ignoring  obj: " << obj << migrateLog;
                return;
            }

            BSONObj it;

            switch ( opstr[0] ) {

            case 'd': {

                if (getThreadName().find(cleanUpThreadName) == 0) {
                    // we don't want to xfer things we're cleaning
                    // as then they'll be deleted on TO
                    // which is bad
                    return;
                }

                // can't filter deletes :(
                _deleted.push_back( ide.wrap() );
                _memoryUsed += ide.size() + 5;
                return;
            }

            case 'i':
                it = obj;
                break;

            case 'u':
                if ( ! Helpers::findById( cc() , _ns.c_str() , ide.wrap() , it ) ) {
                    warning() << "logOpForSharding couldn't find: " << ide << " even though should have" << migrateLog;
                    return;
                }
                break;

            }

            if ( ! isInRange( it , _min , _max , _shardKeyPattern ) )
                return;

            _reload.push_back( ide.wrap() );
            _memoryUsed += ide.size() + 5;
        }

        void xfer( list<BSONObj> * l , BSONObjBuilder& b , const char * name , long long& size , bool explode ) {
            const long long maxSize = 1024 * 1024;

            if ( l->size() == 0 || size > maxSize )
                return;

            BSONArrayBuilder arr(b.subarrayStart(name));

            list<BSONObj>::iterator i = l->begin();

            while ( i != l->end() && size < maxSize ) {
                BSONObj t = *i;
                if ( explode ) {
                    BSONObj it;
                    if ( Helpers::findById( cc() , _ns.c_str() , t, it ) ) {
                        arr.append( it );
                        size += it.objsize();
                    }
                }
                else {
                    arr.append( t );
                }
                i = l->erase( i );
                size += t.objsize();
            }

            arr.done();
        }

        /**
         * called from the dest of a migrate
         * transfers mods from src to dest
         */
        bool transferMods( string& errmsg , BSONObjBuilder& b ) {
            if ( ! _getActive() ) {
                errmsg = "no active migration!";
                return false;
            }

            long long size = 0;

            {
                Client::ReadContext cx( _ns );

                xfer( &_deleted , b , "deleted" , size , false );
                xfer( &_reload , b , "reload" , size , true );
            }

            b.append( "size" , size );

            return true;
        }

        /**
         * Get the disklocs that belong to the chunk migrated and sort them in _cloneLocs (to avoid seeking disk later)
         *
         * @param maxChunkSize number of bytes beyond which a chunk's base data (no indices) is considered too large to move
         * @param errmsg filled with textual description of error if this call return false
         * @return false if approximate chunk size is too big to move or true otherwise
         */
        bool storeCurrentLocs( long long maxChunkSize , string& errmsg , BSONObjBuilder& result ) {
            Client::ReadContext ctx( _ns );
            NamespaceDetails *d = nsdetails( _ns );
            if ( ! d ) {
                errmsg = "ns not found, should be impossible";
                return false;
            }

            const IndexDetails *idx = d->findIndexByPrefix( _shardKeyPattern ,
                                                            true );  /* require single key */

            if ( idx == NULL ) {
                errmsg = (string)"can't find index in storeCurrentLocs" + causedBy( errmsg );
                return false;
            }
            // Assume both min and max non-empty, append MinKey's to make them fit chosen index
            KeyPattern kp( idx->keyPattern() );
            BSONObj min = Helpers::toKeyFormat( kp.extendRangeBound( _min, false ) );
            BSONObj max = Helpers::toKeyFormat( kp.extendRangeBound( _max, false ) );

            BtreeCursor* btreeCursor = BtreeCursor::make( d , *idx , min , max , false , 1 );
            auto_ptr<ClientCursor> cc(
                    new ClientCursor( QueryOption_NoCursorTimeout ,
                            shared_ptr<Cursor>( btreeCursor ) ,  _ns ) );

            // use the average object size to estimate how many objects a full chunk would carry
            // do that while traversing the chunk's range using the sharding index, below
            // there's a fair amount of slack before we determine a chunk is too large because object sizes will vary
            unsigned long long maxRecsWhenFull;
            long long avgRecSize;
            const long long totalRecs = d->stats.nrecords;
            if ( totalRecs > 0 ) {
                avgRecSize = d->stats.datasize / totalRecs;
                maxRecsWhenFull = maxChunkSize / avgRecSize;
                maxRecsWhenFull = std::min( (unsigned long long)(Chunk::MaxObjectPerChunk + 1) , 130 * maxRecsWhenFull / 100 /* slack */ );
            }
            else {
                avgRecSize = 0;
                maxRecsWhenFull = Chunk::MaxObjectPerChunk + 1;
            }
            
            // do a full traversal of the chunk and don't stop even if we think it is a large chunk
            // we want the number of records to better report, in that case
            bool isLargeChunk = false;
            unsigned long long recCount = 0;;
            while ( cc->ok() ) {
                DiskLoc dl = cc->currLoc();
                if ( ! isLargeChunk ) {
                    scoped_spinlock lk( _trackerLocks );
                    _cloneLocs.insert( dl );
                }
                cc->advance();

                // we can afford to yield here because any change to the base data that we might miss is already being
                // queued and will be migrated in the 'transferMods' stage
                if ( ! cc->yieldSometimes( ClientCursor::DontNeed ) ) {
                    cc.release();
                    break;
                }

                if ( ++recCount > maxRecsWhenFull ) {
                    isLargeChunk = true;
                }
            }

            if ( isLargeChunk ) {
                warning() << "can't move chunk of size (approximately) " << recCount * avgRecSize
                          << " because maximum size allowed to move is " << maxChunkSize
                          << " ns: " << _ns << " " << _min << " -> " << _max
                          << migrateLog;
                result.appendBool( "chunkTooBig" , true );
                result.appendNumber( "estimatedChunkSize" , (long long)(recCount * avgRecSize) );
                errmsg = "chunk too big to move";
                return false;
            }

            {
                scoped_spinlock lk( _trackerLocks );
                log() << "moveChunk number of documents: " << _cloneLocs.size() << migrateLog;
            }
            return true;
        }

        bool clone( string& errmsg , BSONObjBuilder& result ) {
            if ( ! _getActive() ) {
                errmsg = "not active";
                return false;
            }

            ElapsedTracker tracker (128, 10); // same as ClientCursor::_yieldSometimesTracker

            int allocSize;
            {
                Client::ReadContext ctx( _ns );
                NamespaceDetails *d = nsdetails( _ns );
                verify( d );
                scoped_spinlock lk( _trackerLocks );
                allocSize = std::min(BSONObjMaxUserSize, (int)((12 + d->averageObjectSize()) * _cloneLocs.size()));
            }
            BSONArrayBuilder a (allocSize);
            
            while ( 1 ) {
                bool filledBuffer = false;
                
                auto_ptr<LockMongoFilesShared> fileLock;
                Record* recordToTouch = 0;

                {
                    Client::ReadContext ctx( _ns );
                    scoped_spinlock lk( _trackerLocks );
                    set<DiskLoc>::iterator i = _cloneLocs.begin();
                    for ( ; i!=_cloneLocs.end(); ++i ) {
                        if (tracker.intervalHasElapsed()) // should I yield?
                            break;
                        
                        DiskLoc dl = *i;
                        
                        Record* r = dl.rec();
                        if ( ! r->likelyInPhysicalMemory() ) {
                            fileLock.reset( new LockMongoFilesShared() );
                            recordToTouch = r;
                            break;
                        }
                        
                        BSONObj o = dl.obj();
                        
                        // use the builder size instead of accumulating 'o's size so that we take into consideration
                        // the overhead of BSONArray indices
                        if ( a.len() + o.objsize() + 1024 > BSONObjMaxUserSize ) {
                            filledBuffer = true; // break out of outer while loop
                            break;
                        }
                        
                        a.append( o );
                    }
                    
                    _cloneLocs.erase( _cloneLocs.begin() , i );
                    
                    if ( _cloneLocs.empty() || filledBuffer )
                        break;
                }
                
                if ( recordToTouch ) {
                    // its safe to touch here because we have a LockMongoFilesShared
                    // we can't do where we get the lock because we would have to unlock the main readlock and tne _trackerLocks
                    // simpler to handle this out there
                    recordToTouch->touch();
                    recordToTouch = 0;
                }
                
            }

            result.appendArray( "objects" , a.arr() );
            return true;
        }

        void aboutToDelete( const Database* db , const DiskLoc& dl ) {
            verify(db);
            Lock::assertWriteLocked(db->name);

            if ( ! _getActive() )
                return;

            if ( ! db->ownsNS( _ns ) )
                return;

            
            // not needed right now
            // but trying to prevent a future bug
            scoped_spinlock lk( _trackerLocks ); 

            _cloneLocs.erase( dl );
        }

        long long mbUsed() const { return _memoryUsed / ( 1024 * 1024 ); }

        bool getInCriticalSection() const {
            scoped_lock l(_mutex);
            return _inCriticalSection;
        }

        void setInCriticalSection( bool b ) {
            scoped_lock l(_mutex);
            _inCriticalSection = b;
            _inCriticalSectionCV.notify_all();
        }

        /**
         * @return true if we are NOT in the critical section
         */
        bool waitTillNotInCriticalSection( int maxSecondsToWait ) {
            verify( !Lock::isLocked() );

            boost::xtime xt;
            boost::xtime_get(&xt, MONGO_BOOST_TIME_UTC);
            xt.sec += maxSecondsToWait;

            scoped_lock l(_mutex);
            while ( _inCriticalSection ) {
                if ( ! _inCriticalSectionCV.timed_wait( l.boost(), xt ) )
                    return false;
            }

            return true;
        }

        bool isActive() const { return _getActive(); }
        
        void doRemove( OldDataCleanup& cleanup ) {

            log() << "waiting to remove documents for " << cleanup.toString() << endl;

            ScopedTicket ticket(&_cleanupTickets);

            cleanup.doRemove();
        }

    private:
        mutable mongo::mutex _mutex; // protect _inCriticalSection and _active
        boost::condition _inCriticalSectionCV;

        bool _inCriticalSection;
        bool _active;

        string _ns;
        BSONObj _min;
        BSONObj _max;
        BSONObj _shardKeyPattern;

        // we need the lock in case there is a malicious _migrateClone for example
        // even though it shouldn't be needed under normal operation
        SpinLock _trackerLocks;

        // disk locs yet to be transferred from here to the other side
        // no locking needed because built initially by 1 thread in a read lock
        // emptied by 1 thread in a read lock
        // updates applied by 1 thread in a write lock
        set<DiskLoc> _cloneLocs;

        list<BSONObj> _reload; // objects that were modified that must be recloned
        list<BSONObj> _deleted; // objects deleted during clone that should be deleted later
        long long _memoryUsed; // bytes in _reload + _deleted

        // this is used to make sure only a certain number of threads are doing cleanup at once.
        mutable TicketHolder _cleanupTickets;

        bool _getActive() const { scoped_lock l(_mutex); return _active; }
        void _setActive( bool b ) { scoped_lock l(_mutex); _active = b; }

    } migrateFromStatus;

    struct MigrateStatusHolder {
        MigrateStatusHolder( const std::string& ns ,
                             const BSONObj& min ,
                             const BSONObj& max ,
                             const BSONObj& shardKeyPattern ) {
            _isAnotherMigrationActive = !migrateFromStatus.start(ns, min, max, shardKeyPattern);
        }
        ~MigrateStatusHolder() {
            if (!_isAnotherMigrationActive) {
                migrateFromStatus.done();
            }
        }

        bool isAnotherMigrationActive() const {
            return _isAnotherMigrationActive;
        }

    private:
        bool _isAnotherMigrationActive;
    };

    void _cleanupOldData( OldDataCleanup cleanup ) {

        Client::initThread((string(cleanUpThreadName) + string("-") +
                                                        OID::gen().toString()).c_str());

        if (!noauth) {
            cc().getAuthorizationManager()->grantInternalAuthorization("_cleanupOldData");
        }

        log() << " (start) waiting to cleanup " << cleanup
              << ", # cursors remaining: " << cleanup.initial.size() << migrateLog;

        int loops = 0;
        Timer t;
        while ( t.seconds() < 900 ) { // 15 minutes
            verify( !Lock::isLocked() );
            sleepmillis( 20 );

            set<CursorId> now;
            ClientCursor::find( cleanup.ns , now );

            set<CursorId> left;
            for ( set<CursorId>::iterator i=cleanup.initial.begin(); i!=cleanup.initial.end(); ++i ) {
                CursorId id = *i;
                if ( now.count(id) )
                    left.insert( id );
            }

            if ( left.size() == 0 )
                break;
            cleanup.initial = left;

            if ( ( loops++ % 200 ) == 0 ) {
                log() << " (looping " << loops << ") waiting to cleanup " << cleanup.ns << " from " << cleanup.min << " -> " << cleanup.max << "  # cursors:" << cleanup.initial.size() << migrateLog;

                stringstream ss;
                for ( set<CursorId>::iterator i=cleanup.initial.begin(); i!=cleanup.initial.end(); ++i ) {
                    CursorId id = *i;
                    ss << id << " ";
                }
                log() << " cursors: " << ss.str() << migrateLog;
            }
        }

        migrateFromStatus.doRemove( cleanup );

        cc().shutdown();
    }

    void cleanupOldData( OldDataCleanup cleanup ) {
        try {
            _cleanupOldData( cleanup );
        }
        catch ( std::exception& e ) {
            log() << " error cleaning old data:" << e.what() << migrateLog;
        }
        catch ( ... ) {
            log() << " unknown error cleaning old data" << migrateLog;
        }
    }

    void logOpForSharding( const char * opstr , const char * ns , const BSONObj& obj , BSONObj * patt ) {
        migrateFromStatus.logOp( opstr , ns , obj , patt );
    }

    void aboutToDeleteForSharding( const Database* db, const NamespaceDetails* nsd, const DiskLoc& dl ) {
        if ( nsd->isCapped() )
            return;
        migrateFromStatus.aboutToDelete( db , dl );
    }

    class TransferModsCommand : public ChunkCommandHelper {
    public:
        TransferModsCommand() : ChunkCommandHelper( "_transferMods" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_transferMods);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            return migrateFromStatus.transferMods( errmsg, result );
        }
    } transferModsCommand;


    class InitialCloneCommand : public ChunkCommandHelper {
    public:
        InitialCloneCommand() : ChunkCommandHelper( "_migrateClone" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_migrateClone);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            return migrateFromStatus.clone( errmsg, result );
        }
    } initialCloneCommand;


    /**
     * this is the main entry for moveChunk
     * called to initial a move
     * usually by a mongos
     * this is called on the "from" side
     */
    class MoveChunkCommand : public Command {
    public:
        MoveChunkCommand() : Command( "moveChunk" ) {}
        virtual void help( stringstream& help ) const {
            help << "should not be calling this directly";
        }

        virtual bool slaveOk() const { return false; }
        virtual bool adminOnly() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::moveChunk);
            out->push_back(Privilege(AuthorizationManager::CLUSTER_RESOURCE_NAME, actions));
        }

        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            // 1. parse options
            // 2. make sure my view is complete and lock
            // 3. start migrate
            //    in a read lock, get all DiskLoc and sort so we can do as little seeking as possible
            //    tell to start transferring
            // 4. pause till migrate caught up
            // 5. LOCK
            //    a) update my config, essentially locking
            //    b) finish migrate
            //    c) update config server
            //    d) logChange to config server
            // 6. wait for all current cursors to expire
            // 7. remove data locally

            // -------------------------------

            // 1.
            string ns = cmdObj.firstElement().str();
            string to = cmdObj["to"].str();
            string from = cmdObj["from"].str(); // my public address, a tad redundant, but safe

            // fromShard and toShard needed so that 2.2 mongos can interact with either 2.0 or 2.2 mongod
            if( cmdObj["fromShard"].type() == String ){
                from = cmdObj["fromShard"].String();
            }

            if( cmdObj["toShard"].type() == String ){
                to = cmdObj["toShard"].String();
            }

            // if we do a w=2 after every write
            bool secondaryThrottle = cmdObj["secondaryThrottle"].trueValue();
            if ( secondaryThrottle ) {
                if ( theReplSet ) {
                    if ( theReplSet->config().getMajority() <= 1 ) {
                        secondaryThrottle = false;
                        warning() << "not enough nodes in set to use secondaryThrottle: "
                                  << " majority: " << theReplSet->config().getMajority()
                                  << endl;
                    }
                }
                else if ( !anyReplEnabled() ) {
                    secondaryThrottle = false;
                    warning() << "secondaryThrottle selected but no replication" << endl;
                }
                else {
                    // master/slave
                    secondaryThrottle = false;
                    warning() << "secondaryThrottle not allowed with master/slave" << endl;
                }
            }

            // Do inline deletion
            bool waitForDelete = cmdObj["waitForDelete"].trueValue();
            if (waitForDelete) {
                log() << "moveChunk waiting for full cleanup after move" << endl;
            }

            BSONObj min  = cmdObj["min"].Obj();
            BSONObj max  = cmdObj["max"].Obj();
            BSONElement shardId = cmdObj["shardId"];
            BSONElement maxSizeElem = cmdObj["maxChunkSizeBytes"];

            if ( ns.empty() ) {
                errmsg = "need to specify namespace in command";
                return false;
            }

            if ( to.empty() ) {
                errmsg = "need to specify shard to move chunk to";
                return false;
            }
            if ( from.empty() ) {
                errmsg = "need to specify shard to move chunk from";
                return false;
            }

            if ( min.isEmpty() ) {
                errmsg = "need to specify a min";
                return false;
            }

            if ( max.isEmpty() ) {
                errmsg = "need to specify a max";
                return false;
            }

            if ( shardId.eoo() ) {
                errmsg = "need shardId";
                return false;
            }

            if ( maxSizeElem.eoo() || ! maxSizeElem.isNumber() ) {
                errmsg = "need to specify maxChunkSizeBytes";
                return false;
            }
            const long long maxChunkSize = maxSizeElem.numberLong(); // in bytes

            if ( ! shardingState.enabled() ) {
                if ( cmdObj["configdb"].type() != String ) {
                    errmsg = "sharding not enabled";
                    return false;
                }
                string configdb = cmdObj["configdb"].String();
                shardingState.enable( configdb );
                configServer.init( configdb );
            }

            MoveTimingHelper timing( "from" , ns , min , max , 6 /* steps */ , errmsg );

            // Make sure we're as up-to-date as possible with shard information
            // This catches the case where we had to previously changed a shard's host by
            // removing/adding a shard with the same name
            Shard::reloadShardInfo();

            // So 2.2 mongod can interact with 2.0 mongos, mongod needs to handle either a conn
            // string or a shard in the to/from fields.  The Shard constructor handles this,
            // eventually we should break the compatibility.

            Shard fromShard( from );
            Shard toShard( to );

            log() << "received moveChunk request: " << cmdObj << migrateLog;

            timing.done(1);

            // 2.
            
            if ( migrateFromStatus.isActive() ) {
                errmsg = "migration already in progress";
                return false;
            }

            DistributedLock lockSetup( ConnectionString( shardingState.getConfigServer() , ConnectionString::SYNC ) , ns );
            dist_lock_try dlk;

            try{
                dlk = dist_lock_try( &lockSetup , (string)"migrate-" + min.toString() );
            }
            catch( LockException& e ){
                errmsg = str::stream() << "error locking distributed lock for migration " << "migrate-" << min.toString() << causedBy( e );
                return false;
            }

            if ( ! dlk.got() ) {
                errmsg = str::stream() << "the collection metadata could not be locked with lock " << "migrate-" << min.toString();
                result.append( "who" , dlk.other() );
                return false;
            }

            BSONObj chunkInfo = BSON("min" << min << "max" << max << "from" << fromShard.getName() << "to" << toShard.getName() );
            configServer.logChange( "moveChunk.start" , ns , chunkInfo );

            ChunkVersion maxVersion;
            ChunkVersion startingVersion;
            string myOldShard;
            {
                scoped_ptr<ScopedDbConnection> conn(
                        ScopedDbConnection::getInternalScopedDbConnection(
                                shardingState.getConfigServer(), 30));

                BSONObj x;
                BSONObj currChunk;
                try{
                    x = conn->get()->findOne(ChunkType::ConfigNS,
                                             Query(BSON(ChunkType::ns(ns)))
                                                  .sort(BSON(ChunkType::DEPRECATED_lastmod() << -1)));

                    currChunk = conn->get()->findOne(ChunkType::ConfigNS,
                                                     shardId.wrap(ChunkType::name().c_str()));
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "aborted moveChunk because could not get chunk data from config server " << shardingState.getConfigServer() << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                maxVersion = ChunkVersion::fromBSON(x, ChunkType::DEPRECATED_lastmod());
                verify(currChunk[ChunkType::shard()].type());
                verify(currChunk[ChunkType::min()].type());
                verify(currChunk[ChunkType::max()].type());
                myOldShard = currChunk[ChunkType::shard()].String();
                conn->done();

                BSONObj currMin = currChunk[ChunkType::min()].Obj();
                BSONObj currMax = currChunk[ChunkType::max()].Obj();
                if ( currMin.woCompare( min ) || currMax.woCompare( max ) ) {
                    errmsg = "boundaries are outdated (likely a split occurred)";
                    result.append( "currMin" , currMin );
                    result.append( "currMax" , currMax );
                    result.append( "requestedMin" , min );
                    result.append( "requestedMax" , max );

                    warning() << "aborted moveChunk because" <<  errmsg << ": " << min << "->" << max
                                      << " is now " << currMin << "->" << currMax << migrateLog;
                    return false;
                }

                if ( myOldShard != fromShard.getName() ) {
                    errmsg = "location is outdated (likely balance or migrate occurred)";
                    result.append( "from" , fromShard.getName() );
                    result.append( "official" , myOldShard );

                    warning() << "aborted moveChunk because " << errmsg << ": chunk is at " << myOldShard
                                      << " and not at " << fromShard.getName() << migrateLog;
                    return false;
                }

                if ( maxVersion < shardingState.getVersion( ns ) ) {
                    errmsg = "official version less than mine?";
                    maxVersion.addToBSON( result, "officialVersion" );
                    shardingState.getVersion( ns ).addToBSON( result, "myVersion" );

                    warning() << "aborted moveChunk because " << errmsg << ": official " << maxVersion
                                      << " mine: " << shardingState.getVersion(ns) << migrateLog;
                    return false;
                }

                // since this could be the first call that enable sharding we also make sure to have the chunk manager up to date
                shardingState.gotShardName( myOldShard );

                // Using the maxVersion we just found will enforce a check - if we use zero version,
                // it's possible this shard will be *at* zero version from a previous migrate and
                // no refresh will be done
                // TODO: Make this less fragile
                startingVersion = maxVersion;
                shardingState.trySetVersion( ns , startingVersion /* will return updated */ );

                if (startingVersion.majorVersion() == 0) {
                   // It makes no sense to migrate if our version is zero and we have no chunks, so return
                   warning() << "moveChunk cannot start migration with zero version" << endl;
                   return false;
                }

                log() << "moveChunk request accepted at version " << startingVersion << migrateLog;
            }

            timing.done(2);

            // 3.

            ShardChunkManagerPtr chunkManager = shardingState.getShardChunkManager( ns );
            verify( chunkManager != NULL );
            BSONObj shardKeyPattern = chunkManager->getKey();
            if ( shardKeyPattern.isEmpty() ){
                errmsg = "no shard key found";
                return false;
            }

            MigrateStatusHolder statusHolder( ns , min , max , shardKeyPattern );
            if (statusHolder.isAnotherMigrationActive()) {
                errmsg = "moveChunk is already in progress from this shard";
                return false;
            }

            {
                // this gets a read lock, so we know we have a checkpoint for mods
                if ( ! migrateFromStatus.storeCurrentLocs( maxChunkSize , errmsg , result ) )
                    return false;

                scoped_ptr<ScopedDbConnection> connTo(
                        ScopedDbConnection::getScopedDbConnection( toShard.getConnString() ) );
                BSONObj res;
                bool ok;
                try{
                    ok = connTo->get()->runCommand( "admin" ,
                                                    BSON( "_recvChunkStart" << ns <<
                                                          "from" << fromShard.getConnString() <<
                                                          "min" << min <<
                                                          "max" << max <<
                                                          "shardKeyPattern" << shardKeyPattern <<
                                                          "configServer" << configServer.modelServer() <<
                                                          "secondaryThrottle" << secondaryThrottle
                                                          ) ,
                                                    res );
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "moveChunk could not contact to: shard "
                                           << to << " to start transfer" << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                connTo->done();

                if ( ! ok ) {
                    errmsg = "moveChunk failed to engage TO-shard in the data transfer: ";
                    verify( res["errmsg"].type() );
                    errmsg += res["errmsg"].String();
                    result.append( "cause" , res );
                    warning() << errmsg << endl;
                    return false;
                }

            }
            timing.done( 3 );

            // 4.
            for ( int i=0; i<86400; i++ ) { // don't want a single chunk move to take more than a day
                verify( !Lock::isLocked() );
                // Exponential sleep backoff, up to 1024ms. Don't sleep much on the first few
                // iterations, since we want empty chunk migrations to be fast.
                sleepmillis( 1 << std::min( i , 10 ) );
                scoped_ptr<ScopedDbConnection> conn(
                        ScopedDbConnection::getScopedDbConnection( toShard.getConnString() ) );
                BSONObj res;
                bool ok;
                try {
                    ok = conn->get()->runCommand( "admin" , BSON( "_recvChunkStatus" << 1 ) , res );
                    res = res.getOwned();
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "moveChunk could not contact to: shard " << to << " to monitor transfer" << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                conn->done();

                LOG(0) << "moveChunk data transfer progress: " << res << " my mem used: " << migrateFromStatus.mbUsed() << migrateLog;

                if ( ! ok || res["state"].String() == "fail" ) {
                    warning() << "moveChunk error transferring data caused migration abort: " << res << migrateLog;
                    errmsg = "data transfer error";
                    result.append( "cause" , res );
                    return false;
                }

                if ( res["state"].String() == "steady" )
                    break;

                if ( migrateFromStatus.mbUsed() > (500 * 1024 * 1024) ) {
                    // this is too much memory for us to use for this
                    // so we're going to abort the migrate
                    scoped_ptr<ScopedDbConnection> conn(
                            ScopedDbConnection::getScopedDbConnection( toShard.getConnString() ) );

                    BSONObj res;
                    conn->get()->runCommand( "admin" , BSON( "_recvChunkAbort" << 1 ) , res );
                    res = res.getOwned();
                    conn->done();
                    error() << "aborting migrate because too much memory used res: " << res << migrateLog;
                    errmsg = "aborting migrate because too much memory used";
                    result.appendBool( "split" , true );
                    return false;
                }

                killCurrentOp.checkForInterrupt();
            }
            timing.done(4);

            // 5.
            {
                // 5.a
                // we're under the collection lock here, so no other migrate can change maxVersion or ShardChunkManager state
                migrateFromStatus.setInCriticalSection( true );
                ChunkVersion myVersion = maxVersion;
                myVersion.incMajor();

                {
                    Lock::DBWrite lk( ns );
                    verify( myVersion > shardingState.getVersion( ns ) );

                    // bump the chunks manager's version up and "forget" about the chunk being moved
                    // this is not the commit point but in practice the state in this shard won't until the commit it done
                    shardingState.donateChunk( ns , min , max , myVersion );
                }

                log() << "moveChunk setting version to: " << myVersion << migrateLog;

                // 5.b
                // we're under the collection lock here, too, so we can undo the chunk donation because no other state change
                // could be ongoing
                {
                    BSONObj res;
                    scoped_ptr<ScopedDbConnection> connTo(
                            ScopedDbConnection::getScopedDbConnection( toShard.getConnString(),
                                                                       35.0 ) );

                    bool ok;

                    try{
                        ok = connTo->get()->runCommand( "admin" ,
                                                        BSON( "_recvChunkCommit" << 1 ) ,
                                                        res );
                    }
                    catch( DBException& e ){
                        errmsg = str::stream() << "moveChunk could not contact to: shard " << toShard.getConnString() << " to commit transfer" << causedBy( e );
                        warning() << errmsg << endl;
                        ok = false;
                    }

                    connTo->done();

                    if ( ! ok ) {
                        log() << "moveChunk migrate commit not accepted by TO-shard: " << res
                              << " resetting shard version to: " << startingVersion << migrateLog;
                        {
                            Lock::GlobalWrite lk;
                            log() << "moveChunk global lock acquired to reset shard version from "
                                    "failed migration" << endl;

                            // revert the chunk manager back to the state before "forgetting" about the chunk
                            shardingState.undoDonateChunk( ns , min , max , startingVersion );
                        }
                        log() << "Shard version successfully reset to clean up failed migration"
                                << endl;

                        errmsg = "_recvChunkCommit failed!";
                        result.append( "cause" , res );
                        return false;
                    }

                    log() << "moveChunk migrate commit accepted by TO-shard: " << res << migrateLog;
                }

                // 5.c

                // version at which the next highest lastmod will be set
                // if the chunk being moved is the last in the shard, nextVersion is that chunk's lastmod
                // otherwise the highest version is from the chunk being bumped on the FROM-shard
                ChunkVersion nextVersion;

                // we want to go only once to the configDB but perhaps change two chunks, the one being migrated and another
                // local one (so to bump version for the entire shard)
                // we use the 'applyOps' mechanism to group the two updates and make them safer
                // TODO pull config update code to a module

                BSONObjBuilder cmdBuilder;

                BSONArrayBuilder updates( cmdBuilder.subarrayStart( "applyOps" ) );
                {
                    // update for the chunk being moved
                    BSONObjBuilder op;
                    op.append( "op" , "u" );
                    op.appendBool( "b" , false /* no upserting */ );
                    op.append( "ns" , ChunkType::ConfigNS );

                    BSONObjBuilder n( op.subobjStart( "o" ) );
                    n.append(ChunkType::name(), Chunk::genID(ns, min));
                    myVersion.addToBSON(n, ChunkType::DEPRECATED_lastmod());
                    n.append(ChunkType::ns(), ns);
                    n.append(ChunkType::min(), min);
                    n.append(ChunkType::max(), max);
                    n.append(ChunkType::shard(), toShard.getName());
                    n.done();

                    BSONObjBuilder q( op.subobjStart( "o2" ) );
                    q.append(ChunkType::name(), Chunk::genID(ns, min));
                    q.done();

                    updates.append( op.obj() );
                }

                nextVersion = myVersion;

                // if we have chunks left on the FROM shard, update the version of one of them as well
                // we can figure that out by grabbing the chunkManager installed on 5.a
                // TODO expose that manager when installing it

                ShardChunkManagerPtr chunkManager = shardingState.getShardChunkManager( ns );
                if( chunkManager->getNumChunks() > 0 ) {

                    // get another chunk on that shard
                    BSONObj lookupKey;
                    BSONObj bumpMin, bumpMax;
                    do {
                        chunkManager->getNextChunk( lookupKey , &bumpMin , &bumpMax );
                        lookupKey = bumpMin;
                    }
                    while( bumpMin == min );

                    BSONObjBuilder op;
                    op.append( "op" , "u" );
                    op.appendBool( "b" , false );
                    op.append( "ns" , ChunkType::ConfigNS );

                    nextVersion.incMinor();  // same as used on donateChunk
                    BSONObjBuilder n( op.subobjStart( "o" ) );
                    n.append(ChunkType::name(), Chunk::genID(ns, bumpMin));
                    nextVersion.addToBSON(n, ChunkType::DEPRECATED_lastmod());
                    n.append(ChunkType::ns(), ns);
                    n.append(ChunkType::min(), bumpMin);
                    n.append(ChunkType::max(), bumpMax);
                    n.append(ChunkType::shard(), fromShard.getName());
                    n.done();

                    BSONObjBuilder q( op.subobjStart( "o2" ) );
                    q.append(ChunkType::name(), Chunk::genID(ns, bumpMin));
                    q.done();

                    updates.append( op.obj() );

                    log() << "moveChunk updating self version to: " << nextVersion << " through "
                          << bumpMin << " -> " << bumpMax << " for collection '" << ns << "'" << migrateLog;

                }
                else {

                    log() << "moveChunk moved last chunk out for collection '" << ns << "'" << migrateLog;
                }

                updates.done();

                BSONArrayBuilder preCond( cmdBuilder.subarrayStart( "preCondition" ) );
                {
                    BSONObjBuilder b;
                    b.append("ns", ChunkType::ConfigNS);
                    b.append("q", BSON("query" << BSON(ChunkType::ns(ns)) <<
                                       "orderby" << BSON(ChunkType::DEPRECATED_lastmod() << -1)));
                    {
                        BSONObjBuilder bb( b.subobjStart( "res" ) );
                        // TODO: For backwards compatibility, we can't yet require an epoch here
                        bb.appendTimestamp(ChunkType::DEPRECATED_lastmod(), maxVersion.toLong());
                        bb.done();
                    }
                    preCond.append( b.obj() );
                }

                preCond.done();

                BSONObj cmd = cmdBuilder.obj();
                LOG(7) << "moveChunk update: " << cmd << migrateLog;

                bool ok = false;
                BSONObj cmdResult;
                try {
                    scoped_ptr<ScopedDbConnection> conn(
                            ScopedDbConnection::getInternalScopedDbConnection(
                                    shardingState.getConfigServer(),
                                    10.0 ) );
                    ok = conn->get()->runCommand( "config" , cmd , cmdResult );
                    conn->done();
                }
                catch ( DBException& e ) {
                    warning() << e << migrateLog;
                    ok = false;
                    BSONObjBuilder b;
                    e.getInfo().append( b );
                    cmdResult = b.obj();
                }

                if ( ! ok ) {

                    // this could be a blip in the connectivity
                    // wait out a few seconds and check if the commit request made it
                    //
                    // if the commit made it to the config, we'll see the chunk in the new shard and there's no action
                    // if the commit did not make it, currently the only way to fix this state is to bounce the mongod so
                    // that the old state (before migrating) be brought in

                    warning() << "moveChunk commit outcome ongoing: " << cmd << " for command :" << cmdResult << migrateLog;
                    sleepsecs( 10 );

                    try {
                        scoped_ptr<ScopedDbConnection> conn(
                                ScopedDbConnection::getInternalScopedDbConnection(
                                        shardingState.getConfigServer(),
                                        10.0 ) );

                        // look for the chunk in this shard whose version got bumped
                        // we assume that if that mod made it to the config, the applyOps was successful
                        BSONObj doc = conn->get()->findOne(ChunkType::ConfigNS,
                                                           Query(BSON(ChunkType::ns(ns)))
                                                               .sort(BSON(ChunkType::DEPRECATED_lastmod() << -1)));

                        ChunkVersion checkVersion =
                            ChunkVersion::fromBSON(doc[ChunkType::DEPRECATED_lastmod()]);

                        if ( checkVersion.isEquivalentTo( nextVersion ) ) {
                            log() << "moveChunk commit confirmed" << migrateLog;

                        }
                        else {
                            error() << "moveChunk commit failed: version is at"
                                            << checkVersion << " instead of " << nextVersion << migrateLog;
                            error() << "TERMINATING" << migrateLog;
                            dbexit( EXIT_SHARDING_ERROR );
                        }

                        conn->done();

                    }
                    catch ( ... ) {
                        error() << "moveChunk failed to get confirmation of commit" << migrateLog;
                        error() << "TERMINATING" << migrateLog;
                        dbexit( EXIT_SHARDING_ERROR );
                    }
                }

                migrateFromStatus.setInCriticalSection( false );

                // 5.d
                configServer.logChange( "moveChunk.commit" , ns , chunkInfo );
            }

            migrateFromStatus.done();
            timing.done(5);

            {
                // 6.
                OldDataCleanup c;
                c.secondaryThrottle = secondaryThrottle;
                c.ns = ns;
                c.min = min.getOwned();
                c.max = max.getOwned();
                c.shardKeyPattern = shardKeyPattern.getOwned();
                ClientCursor::find( ns , c.initial );

                if (!waitForDelete) {
                    // 7.
                    log() << "forking for cleanup of chunk data" << migrateLog;
                    boost::thread t( boost::bind( &cleanupOldData , c ) );
                }
                else {
                    // 7.
                    log() << "doing delete inline for cleanup of chunk data" << migrateLog;
                    c.doRemove();
                }
            }
            timing.done(6);

            return true;

        }

    } moveChunkCmd;
class TestLatencyCommand : public Command {
        
    public:
        TestLatencyCommand() : Command( "testLatency" ) {}
        virtual void help( stringstream& help ) const {
            help << "should not be calling this directly";
        }

        virtual bool slaveOk() const { return true; }
        virtual bool adminOnly() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::testLatency);
            out->push_back(Privilege(AuthorizationManager::CLUSTER_RESOURCE_NAME, actions));
        }


        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
		string to = cmdObj["to"].str();
                string ns = cmdObj["ns"].str();
		
		//sleepmillis( 20 );
                BSONObj res;
		log() << "TestLatency " <<to<<endl;
		Timer t;
		scoped_ptr<ScopedDbConnection> toconn(ScopedDbConnection::getScopedDbConnection(to) );
		toconn->get()->runCommand( "admin" , 
				BSON("ping" << ns) ,
				res
				);
		result.append("millis", t.millis());
		return true;
	}
}TestLatencyCommand;

    /**
     * this is the main entry for moveData
     * called to initiate a move
     * usually by a mongos
     * this is called on the "to" side
     */
    class MoveDataCommand : public Command {
        
    public:
        MoveDataCommand() : Command( "moveData" ) {}
        virtual void help( stringstream& help ) const {
            help << "should not be calling this directly";
        }

        virtual bool slaveOk() const { return true; }
        virtual bool adminOnly() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::moveData);
            out->push_back(Privilege(AuthorizationManager::CLUSTER_RESOURCE_NAME, actions));
        }

	 bool checkAndExtractArgs(BSONObj migrateParams, string& errmsg,
                                    string& ns, int& shardID, int& numChunks,int& numShards, BSONObj& proposedKey, BSONObj& globalMin, BSONObj& globalMax,
                                    vector<BSONObj>& splitPoints, vector<int>& assignments, vector<string>& removedReplicas) {
            //printLogID();
            log()<<"====Arg extraction====="<<endl;
            //namespace
            ns = migrateParams["ns"].String();
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
            log()<<"namespace done "<< ns <<endl;

            //shard id
            shardID = migrateParams["shardID"].Int();

            //printLogID();
            log()<<"shard id done " << shardID <<endl;

            //number of chunks
            numChunks = migrateParams["numChunks"].Int();
             //number of shards
            numShards = migrateParams["numShards"].Int();

            //printLogID();
            log()<<"numChunks done "<< numChunks <<endl;
	    log()<<"numShards done "<< numShards <<endl;

            //proposed key
            proposedKey = migrateParams["proposedKey"].Obj();
            if ( proposedKey.isEmpty() ) {
                errmsg = "no shard key";
                return false;
            }

            //printLogID();
            log()<<"proposedKey done " << proposedKey <<endl;

            //global min
            globalMin = migrateParams["globalMin"].Obj();
            if ( globalMin.isEmpty() ) {
                errmsg = "no global min";
                return false;
            }

            //printLogID();
            log()<<"globalMin done " << globalMin <<endl;

            //global max
            globalMax = migrateParams["globalMax"].Obj();
            if ( globalMax.isEmpty() ) {
                errmsg = "no global max";
                return false;
            }

            //printLogID();
            log()<<"globalMax done "<< globalMax <<endl;


            //split points
            vector<BSONElement> splitPointsRaw = migrateParams["splitPoints"].Array();
            for (vector<BSONElement>::iterator point = splitPointsRaw.begin(); point != splitPointsRaw.end(); point++) {
                splitPoints.push_back((*point).Obj());
            }

            //printLogID();
            log()<<"splitPoints done"<<endl;


            //assignments
            vector<BSONElement> assignmentsRaw = migrateParams["assignments"].Array();
            for (vector<BSONElement>::iterator assignment = assignmentsRaw.begin(); assignment != assignmentsRaw.end(); assignment++){
               assignments.push_back((*assignment).Int());
            }

            //printLogID();
            log()<<"assignments done"<<endl;

            //removed replicas
            vector<BSONElement> removedReplicasRaw = migrateParams["removedReplicas"].Array();
            for (vector<BSONElement>::iterator removedReplica = removedReplicasRaw.begin(); removedReplica != removedReplicasRaw.end(); removedReplica++){
               removedReplicas.push_back((*removedReplica).String());
            }

            //printLogID();
            cout<<"removedReplicas done"<<endl;
            return true;
        }
	BSONObj getRangeAsBSON(const char* key, BSONObj min, BSONObj max)
            {
                BSONElement minElem = min[key];
                BSONElement maxElem = max[key];

                BSONObjBuilder b;
                BSONObjBuilder sub(b.subobjStart(key));
                if (minElem.type() == MinKey)
                    sub.appendAs(maxElem, "$lt");
                else if (maxElem.type() == MaxKey)
                    sub.appendAs(minElem, "$gte");
                else
                {
                    sub.appendAs(minElem, "$gte");
                    sub.appendAs(maxElem, "$lt");
                }

                BSONObj subObj = sub.done();
                BSONObj range = b.done().getOwned();
                return range;
            }
            BSONObj getFetchedDataAsBSON(BSONObj range, long long count) 
            {

                 BSONObjBuilder b;
                 b.append("count",count);
                 b.append("range",range);

                 BSONObj fetchedData = b.done().getOwned();
                 return fetchedData;     
            }

            void getMinMaxAsBSON(BSONObj range, BSONObj proposedKey, BSONObj& min, BSONObj& max)
             
            {
                const char *key = proposedKey.firstElement().fieldName();
                BSONObj sub = range[key].Obj();
                BSONElement maxElem = sub["$lt"];
                BSONElement minElem = sub["$gte"];
                cout<<"[WWT] get sub="<<sub.toString()<<endl;
                cout<<"[WWT] get min="<<minElem.toString()<<endl;
		cout<<"[WWT] get max="<<maxElem.toString()<<endl;

                if(maxElem.eoo() && minElem.eoo()){
                   
                      max = ShardKeyPattern(proposedKey).globalMax();
                      min = ShardKeyPattern(proposedKey).globalMin();
                }
                else if (maxElem.eoo() && !minElem.eoo()){
                      
                      max = ShardKeyPattern(proposedKey).globalMax();
                      BSONObjBuilder b;
                      b.appendAs(minElem, key);
                      min =b.obj();
                }
                else if (!maxElem.eoo() && minElem.eoo()){
                      min = ShardKeyPattern(proposedKey).globalMin();
                      BSONObjBuilder b;
                      b.appendAs(maxElem, key);
                      max =b.obj();
                }
                else{
		      BSONObjBuilder b1;
                      b1.appendAs(minElem, key);
                      min =b1.obj();
                      BSONObjBuilder b2;
                      b2.appendAs(maxElem, key);
                      max =b2.obj();
                }
                
            }
 
        void print(vector< std::map<BSONObj, vector<BSONObj> > >& threadsBuckets)
        {
            typedef map<BSONObj, vector<BSONObj> >::iterator it_type;
            for(unsigned int i=0;i<threadsBuckets.size();i++)
            {
                 std::map<BSONObj, vector<BSONObj> > map1 = threadsBuckets[i];
                 log() << "[WWT Migrate] bucket[" << i << "] : --------" <<endl;
		 for(it_type it = map1.begin() ; it!= map1.end(); it++) 
                 {
                     log() << "from " << (it->first)["from"].str() << " count " << (it->first)["count"].Long() << endl ;
                     vector<BSONObj> ranges = it->second;
                     for (unsigned int j = 0; j< ranges.size() ; j++ ){
                           log() << "range: " << ranges[j].toString() << "\t";
                     }
                     log() << endl;
                 }
	    }
        } 

	void collectFetchedData( vector< std::map<BSONObj, vector<BSONObj> > >& threadsBuckets,
                                 vector<BSONObj>& splitPoints, vector<int>& assignment, vector<string>& removedReplicas, string& ns,
				 int& shardID, int& numChunks, int& numShards, 
				 BSONObj& proposedKey, BSONObj& globalMax, BSONObj& globalMin)
	{
                std::map<string , vector<BSONObj> > fromList;
		const char *key = proposedKey.firstElement().fieldName();
                vector<BSONObj>::iterator it = splitPoints.begin();
                BSONObj prev;

		long long sourceCount;

		for (int i = 0; i < numChunks; i++)
		{
                    BSONObj min = i > 0 ? prev : globalMin;
                    BSONObj max = i == numChunks - 1 ? globalMax : *it;
                    BSONObj range = getRangeAsBSON(key, min, max);
                    //cout << "[WWT] Range:" << range.toString() << endl;

                    //If I am the destination node
                    if (assignment[i] == shardID)
		    {
                        for (int j = 0; j < numShards; j++)
                        {
                            if (j != shardID)
                            {
                                scoped_ptr<ScopedDbConnection> fromconn(ScopedDbConnection::getScopedDbConnection(removedReplicas[j] ) );
                                while (true)
				{
					try
					{
						sourceCount = fromconn->get()->count(ns, range, QueryOption_SlaveOk);
			 			break;
					}
					catch (DBException e)
					{
						continue;
					}
				}
                                fromconn->done();
                                if (sourceCount > 0) 
                                {
					string key(removedReplicas[j]);
                                        BSONObj singleRange = getFetchedDataAsBSON(range,sourceCount);
                                       
                                        if(fromList.find(key) == fromList.end()){
						vector<BSONObj> ranges;
						ranges.push_back(singleRange);
						fromList[key] = ranges;
					} else {
						fromList[key].push_back(singleRange);
					}

                                }
                            }//end if (j != shardID)
			}//end for

                    }//end if (assignment[i] == shardID)
		    if (i < numChunks - 1)
		    {
                    	prev = *it;
                    	it++;
	            }

		}//end for
                
                //initial  threadsBuckets
                //log() << "[WWT] initial threads buckets" <<endl;
                typedef map<string, vector<BSONObj> >::iterator it_type;

	        for(it_type iterator = fromList.begin(); iterator!=fromList.end(); iterator++){
		    vector<BSONObj> ranges = iterator->second;
		    string from = iterator->first;
                    long long totalCount =0;
		    for(vector<BSONObj>::iterator it=ranges.begin();it!=ranges.end(); it++)
                    {
                        totalCount += (*it)["count"].Long();		
                    }
                    //log() << "[WWT] from = " << from << " count = " << totalCount << endl;
                
                    BSONObjBuilder b;
                    b.append("count", totalCount);
                    b.append("from", from);
                    BSONObj fromObj = b.done().getOwned();
                
                    std::map<BSONObj, vector<BSONObj> >fromMap;
                    fromMap.insert(std::make_pair(fromObj, ranges));
                    threadsBuckets.push_back(fromMap);
                }

                log()<< "[WWT]-------------collect from list -------------------" << endl;
		print(threadsBuckets);

	}


	void matchThreadandFromNodes(unsigned int& numThreads, vector< std::map<BSONObj, vector<BSONObj> > >& threadsBuckets, BSONObj proposedKey, string ns,BSONElement maxSizeElem) 
        {
            /*
            //strategy 1
            // start matching to guarantee each thread has a map to fetch
            while(threadsBuckets.size() != numThreads ) 
            { 
                if(threadsBuckets.size() > numThreads) 
                {
                    //merge the vector
                    mergeFromList(threadsBuckets);
                }
                else if (threadsBuckets.size() < numThreads)
                {
                    //split the vector
                    splitFromList(threadsBuckets,proposedKey,ns);
                }
                else {
                   break;
		} 
            }
            */
            //strategy 2
            if(threadsBuckets.size() > numThreads) 
            {
                    while(threadsBuckets.size() != numThreads ) 
                    { 
                        //merge the vector
                        mergeFromList(threadsBuckets);
                    }
            }
            else if (threadsBuckets.size() < numThreads)
            {
                    //split the vector
                    long long totalCount = 0;
                    for(unsigned int i =0;i<threadsBuckets.size();i++)
                    {
                        totalCount +=(threadsBuckets[i].begin()->first)["count"].Long();
                    }
                    
                    unsigned int threadsArray[threadsBuckets.size()];
                    vector<vector<map<BSONObj, vector<BSONObj> > > > candidateList;
                    vector<map<BSONObj, vector<BSONObj> > > candidate;
                    for(unsigned int i =0;i<threadsBuckets.size();i++)
                    {
                        
                        threadsArray[i] = (int)round(( (double)(threadsBuckets[i].begin()->first)["count"].Long()) / (double)totalCount * numThreads);
                        log() << "[WWT] threadsArray[" << i <<"]=" << threadsArray[i] 
                                    <<" count = " << (threadsBuckets[i].begin()->first)["count"].Long() 
                                    << " totalCount = " << totalCount << endl;

			if(threadsArray[i]==0){
                            threadsArray[i] = 1;
                        }
                        
                        
                        //candidate.push_back(threadsBuckets[i]);

                       // while(threadsArray[i]!=candidate.size()){
                            splitFromList(candidate, threadsBuckets[i], threadsArray[i], proposedKey,ns, maxSizeElem);
                           // log() << "[WWT] candidate list size=" << candidate.size() << endl;
                            //splitFromList(candidate, proposedKey,ns, maxSizeElem);
                        //} 
                        candidateList.push_back(candidate);
                    }
                    threadsBuckets.clear();
                    //for(unsigned int i = 0; i< candidateList.size();i++)
                    //{
                    //    vector<map<BSONObj, vector<BSONObj> > > candidate = candidateList[i];
                        for(unsigned int j = 0; j< candidate.size();j++)
                        {
                            threadsBuckets.push_back(candidate[j]);
                        }
                   // }
                   log()<< "[WWT]-------------after matching -------------------" << endl;
		   print(threadsBuckets);
            }
            else {
               return;
	    } 
	}

        void splitFromList(vector< std::map<BSONObj, vector<BSONObj> > >& candidate ,std::map<BSONObj, vector<BSONObj> > & fromList, int numThreads, BSONObj proposedKey, string ns,BSONElement maxSizeElem) {
             const char *key_char = proposedKey.firstElement().fieldName();
             if(fromList.size() != 1){
                  log()<<"[WWT] error in splitFromList";
                  return;
             } else {
                 map<BSONObj, vector<BSONObj> >::iterator iterator= fromList.begin();
                 vector<BSONObj> ranges = iterator->second;
		 BSONObj from = iterator->first;
                 string fromStr = from["from"].str();

                 long long totalCount = from["count"].Long();
                 if(numThreads )


		 for(vector<BSONObj>::iterator it=ranges.begin();it!=ranges.end(); it++)
                 {
                     
                     BSONObj rangeObj = *it ;
                     BSONObj range = rangeObj["range"].Obj();
                     long long rangeCount = rangeObj["count"].Long();

                     int numRangeThread = (int)round( (double)rangeCount / (double)totalCount * numThreads);
                     if(numRangeThread < 1 ){
                         numRangeThread = 1;
                     }
                     log() << "[WWT] range " << rangeObj.toString() << " has " << numRangeThread << " threads" <<endl;
                     if(numRangeThread > 1){
                          log() << "[WWT] checkpoint1" <<endl;
                          BSONObjSet rangeSet;
                
                          BSONObj min, max;
                          log() << "[WWT] checkpoint2" <<endl;
                          getMinMaxAsBSON(range, proposedKey, min, max);

                          log() << "[WWT Migrate] start split Vector = " << fromStr 
                                << " key" << proposedKey.toString() 
                                << " range " << range.toString() 
                                << " min " << min.toString() 
                                << " max " << max.toString()<<endl;

                          scoped_ptr<ScopedDbConnection> conn(
                                      ScopedDbConnection::getInternalScopedDbConnection(fromStr));

             	          BSONObj splitResult;
             	          BSONObjBuilder cmd;
             	          cmd.append( "splitVector" , ns );//TO-DO make sure this is the right ns
             	          cmd.append( "keyPattern" , proposedKey );
	                  cmd.append( "min" , min );
        	          cmd.append( "max" , max );
             	          cmd.append( "range", range);
             	          cmd.append( "maxChunkSizeBytes" , maxSizeElem.Int() );
             	          cmd.append( "maxSplitPoints" , numRangeThread);
             	          //cmd.append( "maxChunkObjects" , maxObjs ); don't need this, in SplitVector deal with this
             	          cmd.appendBool( "subSplit" , true);
             	          BSONObj splitCmdObj = cmd.obj();
        
             	          if ( ! conn->get()->runCommand( "admin" , splitCmdObj , splitResult )) {
                            conn->done();
                            log() << "[WWT Migrate] Pick split Vector cmd failed\n";
                            return ;
             	          }
        
            	          log() << "[WWT Migrate] Pick split Vector cmd done\n"; 
                          BSONObjIterator it( splitResult.getObjectField( "splitKeys" ) );
             	          BSONObj prev;
             	          for(int j = 0; j < numRangeThread ; j++){
                            BSONObj current;
                            if(it.more()){
                                   current = it.next().Obj().getOwned();
                            } else {
                                   current = max;
                            }
                            BSONObj local_min = j > 0 ? prev : min;
		            BSONObj local_max = j == numRangeThread-1 ? max : current;
                            BSONObj subRange = getRangeAsBSON(key_char, local_min, local_max);
                            log() << "[WWT Migrate] subRange:" << subRange.toString() << endl;
                  
                            BSONObjBuilder b;
                            b.append("range",subRange);
                            b.append("count",rangeCount /numRangeThread);
                            vector<BSONObj> ranges;
                            ranges.push_back(b.done().getOwned());
                            map<BSONObj, vector<BSONObj> > map;
			    map.insert(std::make_pair(from, ranges));
                            candidate.push_back(map);
                            
                            //rangeSet.insert( range.getOwned() );
             	            prev = current;
             	          } // end interation of subRange
                     }//end numRangeThread>1
                     else { //numRangeThread == 1
                          vector<BSONObj> ranges;
                          ranges.push_back(rangeObj.getOwned());
                          map<BSONObj, vector<BSONObj> > map;
			  map.insert(std::make_pair(from, ranges));
                          candidate.push_back(map);
                         
                     }
		
                 }//end for range
                 log() << "----------After Split---------" <<endl;
                 print(candidate);
             }//end else         
        }


/*
	void splitFromList(vector< std::map<BSONObj, vector<BSONObj> > >& threadsBuckets, BSONObj proposedKey, string ns,BSONElement maxSizeElem) {
            //find the largest from nodes
            log() << "[WWT Migrate] split begin" << endl;
            typedef std::map<BSONObj, vector<BSONObj> >::iterator it_type;

            unsigned int l = 0;
            long long largestCount = 0;

            for(unsigned int i= 0; i < threadsBuckets.size() ; i++)
            {
                std::map<BSONObj, vector<BSONObj> > fromList = threadsBuckets[i];
                long long count = 0;
                for(it_type it = fromList.begin() ; it!= fromList.end(); it++) 
                {
                      count += (it->first)["count"].Long();
                }
                if(count > largestCount) 
                {
                    largestCount = count;
                    l = i;
                }
            }

            log() << "[WWT Migrate] largestCount = " << largestCount << " in " << l <<endl;
            //split this largest one and get two new ones
            std::map<BSONObj, vector<BSONObj> > from1;
            std::map<BSONObj, vector<BSONObj> > from2;

            if(!splitOneFrom(threadsBuckets[l], from1, from2, proposedKey,ns, maxSizeElem)){
                  log()<<"[WWT Migrate] something wrong with splitOneFrom" << endl;
                  return;
            }

            //delete largest, insert two new ones
            threadsBuckets.erase(threadsBuckets.begin()+l);
            threadsBuckets.push_back(from1);
            threadsBuckets.push_back(from2);
            
            log() << "[WWT Migrate] after split :" << endl;
	    print(threadsBuckets);
        }
 
        bool splitOneFrom(map<BSONObj, vector<BSONObj> >& old, map<BSONObj, vector<BSONObj> >& new1,map<BSONObj, vector<BSONObj> >& new2, BSONObj proposedKey, string ns,BSONElement maxSizeElem)
        {
            //we only split the first one of the map because there is only one element in the map in split case
            // the initial threadsBucket are grouped by from nodes, so there won't be two from nodes in the same map
           const char *key_char = proposedKey.firstElement().fieldName();
           string fromStr = (old.begin()->first)["from"].str();
            //if there is only one range in the map's range vector, we need to send a SplitVector command
            if(old.begin()->second.size() == 1 ) {
                //send SplitVector Command
                BSONObjSet rangeSet;
                
                long long count = (old.begin()->first)["count"].Long();

                BSONObj range = (old.begin()->second)[0]["range"].Obj();
                BSONObj min, max;
                getMinMaxAsBSON(range, proposedKey, min, max);

                log() << "[WWT Migrate] start split Vector = " << fromStr 
                      << " key" << proposedKey.toString() 
                      << " range " << range.toString() 
                      << " min " << min.toString() 
                      << " max " << max.toString()<<endl;

                scoped_ptr<ScopedDbConnection> conn(
                            ScopedDbConnection::getInternalScopedDbConnection(fromStr));

             	BSONObj splitResult;
             	BSONObjBuilder cmd;
             	cmd.append( "splitVector" , ns );//TO-DO make sure this is the right ns
             	cmd.append( "keyPattern" , proposedKey );
	        cmd.append( "min" , min );
        	cmd.append( "max" , max );
             	cmd.append( "range", range);
             	cmd.append( "maxChunkSizeBytes" , maxSizeElem.Int() );
             	cmd.append( "maxSplitPoints" , 2);
             	//cmd.append( "maxChunkObjects" , maxObjs ); don't need this, in SplitVector deal with this
             	cmd.appendBool( "subSplit" , true);
             	BSONObj splitCmdObj = cmd.obj();
        
             	if ( ! conn->get()->runCommand( "admin" , splitCmdObj , splitResult )) {
                  conn->done();
                  log() << "[WWT Migrate] Pick split Vector cmd failed\n";
                  return false;
             	}
        
            	log() << "[WWT Migrate] Pick split Vector cmd done\n";
                conn->done();
                BSONObjBuilder b2;
                b2.append("from",fromStr);
                b2.append("count",count/2);

                BSONObj fromNode=b2.done().getOwned();
   

		BSONObjIterator it( splitResult.getObjectField( "splitKeys" ) );
             	BSONObj prev;
             	for(int j = 0; j < 2 ; j++){
                  BSONObj current;
                  if(it.more()){
                         current = it.next().Obj().getOwned();
                  } else {
                         current = max;
                  }
                  BSONObj local_min = j > 0 ? prev : min;
		  BSONObj local_max = j == 2-1 ? max : current;
                  BSONObj range = getRangeAsBSON(key_char, local_min, local_max);
                  log() << "[WWT Migrate] subRange:" << range.toString() << endl;
                  
                  BSONObjBuilder b;
                  b.append("range",range);
                  b.append("count",count/2);
                  vector<BSONObj> ranges;
                  ranges.push_back(b.done().getOwned());

                  if( j == 0 ) {
                     new1.insert(std::make_pair(fromNode, ranges));
                  }
                  else {
                     new2.insert(std::make_pair(fromNode, ranges));
                  }
                  
                  rangeSet.insert( range.getOwned() );
             	  prev = current;
             	}
             	
                //log()<<"[WWT_TIME] SplitVector Finish  "<<endl;
            }
            else  {
            //if there is more than one range in the map's range vector, we need to perform Balance Partition algorithm
            //since N(sum of rows) is much bigger than n(# of ranges), we only use approximation algo(greedy) here
            //more details in http://en.wikipedia.org/wiki/Partition_problem
                vector<BSONObj> team = old.begin()->second;
                vector<BSONObj> team1;
                vector<BSONObj> team2;
                //TO-DO sort team
                std::sort(team.begin(),team.end(), compareRange);
                
                long long team1Count=0;
                long long team2Count=0;

                for(unsigned int i=0;i<team.size(); i++){
		    if(team1Count<=team2Count){
                       team1.push_back(team[i]);
                       team1Count += team[i]["count"].Long(); 
                    }
                    else{
                       team2.push_back(team[i]);
                       team2Count += team[i]["count"].Long(); 
                    }                   
                }
                BSONObjBuilder b1;
                b1.append("from",fromStr);
                b1.append("count",team1Count);
                BSONObj fromNode1 = b1.done().getOwned();
                new1.insert(std::make_pair(fromNode1, team1));

                BSONObjBuilder b2;
                b2.append("from",fromStr);
                b2.append("count",team2Count);
                BSONObj fromNode2 = b2.done().getOwned();
                new2.insert(std::make_pair(fromNode2, team2));

            }
		typedef std::map<BSONObj, vector<BSONObj> >::iterator it_type;
		log()<< "[WWT Migrate] new1"<< endl;
		for(it_type it = new1.begin() ; it!= new1.end(); it++) 
                 {
                     log() << "from " << (it->first)["from"].str() << " count " << (it->first)["count"].Long() ;
                     vector<BSONObj> ranges = it->second;
                     for (unsigned int j = 0; j< ranges.size() ; j++ ){
                           log() << "range: " << ranges[j].toString();
                     }
                     log() << endl;
                 }

                
                log()<< "[WWT Migrate] new2"<< endl;
		for(it_type it = new2.begin() ; it!= new2.end(); it++) 
                 {
                     log() << "from " << (it->first)["from"].str() << " count " << (it->first)["count"].Long() ;
                     vector<BSONObj> ranges = it->second;
                     for (unsigned int j = 0; j< ranges.size() ; j++ ){
                           log() << "range: " << ranges[j].toString();
                     }
                     log() << endl;
                 }
                 return true;
        }

        static bool compareRange(const BSONObj& range1, const BSONObj& range2)
        {
            long long count1 = range1["count"].Long();
            long long count2 = range2["count"].Long();
           
            return count1>count2;
        }
*/
        void mergeFromList(vector< std::map<BSONObj, vector<BSONObj> > >& threadsBuckets) {
            //find the two smallest from nodes
            log() << "[WWT Migrate] merge begin" << endl;
	    typedef std::map<BSONObj, vector<BSONObj> >::iterator it_type;

            unsigned int s=0;
	    unsigned int s2=0;

            long long smallestCount = LLONG_MAX;
	    long long smallestCount2 = LLONG_MAX;
	
            
	    for(unsigned int i= 0; i < threadsBuckets.size() ; i++)
            {
		std::map<BSONObj, vector<BSONObj> > fromList = threadsBuckets[i];
                long long count = 0;
                for(it_type it = fromList.begin() ; it!= fromList.end(); it++) 
                {
                      count += (it->first)["count"].Long();
                }
                
            	if(count < smallestCount )
                {
			s2 = s;
			s = i;
			
                        smallestCount2= smallestCount;	
			smallestCount= count;
		}
                else 
                {
			if(count < smallestCount2 )
                        {
				s2 = i;
				smallestCount2 = count;
			}
		}
				
	    }
            log() << "[WWT Migrate] smallestCount = " << smallestCount << " in " << s <<endl;
 	    log() << "[WWT Migrate] smallestCount2 = " << smallestCount2 << " in " << s2 <<endl;
            
            //merge them

            // take all element in second smallest map and put it into smallest map
            std::map<BSONObj, vector<BSONObj> > map2 = threadsBuckets[s2];

            for(it_type it = map2.begin() ; it!= map2.end(); it++) 
            {
                     threadsBuckets[s].insert(std::make_pair(it->first, it->second)); 
            }
            //delete s2
            threadsBuckets.erase(threadsBuckets.begin()+s2);

            log() << "[WWT Migrate] new threads Buckets:" << endl;
	    print(threadsBuckets);
            
        }

        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            // 1.
            Timer t;
            log()<<"[WWT Migrate] Move Data Starts"<<endl;

            BSONObj migrateParams = cmdObj["para"].Obj().getOwned();

            //declare the arguments we want
            string ns;                                          //namespace
            int shardID;                                        //the id for this shard
            BSONObj proposedKey;                                //the proposed key
            BSONObj globalMin;                                  //global min
            BSONObj globalMax;                                  //global max
            int numChunks;                                      //number of chunks
            int numShards;
            vector<BSONObj> splitPoints;                        //split points
            vector<int> assignments;                            //the new assignments for chunks
            vector<string> removedReplicas;                     //the other removed replicas 

            //extract all parameters and check
            if( !checkAndExtractArgs(migrateParams, errmsg,
                                        ns, shardID, numChunks,numShards, proposedKey, globalMin, globalMax,
                                        splitPoints, assignments,removedReplicas)) {
                return false;
            }   
            string key(proposedKey.firstElement().fieldName());
            bool secondaryThrottle = cmdObj["secondaryThrottle"].trueValue();
            if ( secondaryThrottle ) {
                if ( theReplSet ) {
                    if ( theReplSet->config().getMajority() <= 1 ) {
                        secondaryThrottle = false;
                        warning() << "not enough nodes in set to use secondaryThrottle: "
                                  << " majority: " << theReplSet->config().getMajority()
                                  << endl;
                    }
                }
                else if ( !anyReplEnabled() ) {
                    secondaryThrottle = false;
                    warning() << "secondaryThrottle selected but no replication" << endl;
                }
                else {
                    // master/slave
                    secondaryThrottle = false;
                    warning() << "secondaryThrottle not allowed with master/slave" << endl;
                }
            }
	   
            BSONElement maxSizeElem = cmdObj["maxChunkSizeBytes"];

            if ( maxSizeElem.eoo() || ! maxSizeElem.isNumber() ) {
                errmsg = "need to specify maxChunkSizeBytes";
                return false;
            }

            if ( ! shardingState.enabled() ) {
                if ( cmdObj["configdb"].type() != String ) {
                    errmsg = "sharding not enabled";
                    return false;
                }
                string configdb = cmdObj["configdb"].String();
                shardingState.enable( configdb );
                configServer.init( configdb );
            }
             Timer t1;
	     unsigned int numThreads = cmdObj["numThreads"].Int();

             if(numThreads <= 0){
                  numThreads = 1;
             }
             log() << "[WWT] assign to me threads = " << numThreads << endl;

             vector< std::map<BSONObj, vector<BSONObj> > >threadsBuckets;
             //collect fetched data
	     collectFetchedData(threadsBuckets, splitPoints, assignments, removedReplicas, ns,
				 shardID, numChunks,numShards, 
				 proposedKey, globalMax, globalMin); 
             if(threadsBuckets.empty())
             {
                 log() << "[WWT Migrate] no data need to be migrated to me" << endl;
                 return true;
             } 

             matchThreadandFromNodes(numThreads, threadsBuckets,proposedKey, ns, maxSizeElem);

	    log() << "[WWT Migrate] After matching threads and Buckets:" << endl;
            print(threadsBuckets);
	     

             vector<shared_ptr<boost::thread> > migrateThreads;
             for(unsigned int i=0;i<threadsBuckets.size();i++){
                 migrateThreads.push_back(shared_ptr<boost::thread>(new boost::thread (boost::bind(&MoveDataCommand::singleMigrate, this, boost::ref( threadsBuckets[i]) , ns,key, i ))));
             }

	     for (unsigned i = 0; i < migrateThreads.size(); i++) {
			migrateThreads[i]->join();
             }
                
	     DBClientConnection::setLazyKillCursor(true);
             log()<<"[WWT_TIME] FetchingData "<<  "to " <<removedReplicas[shardID] <<"Finish in "<<t1.millis()<<endl;
             return true;

	   }

            void singleMigrate( std::map < BSONObj, vector<BSONObj> >& fromList, string ns,string key, int i)
           {
		Timer t;
                long long totalCount = 0;
                //threadName+=range.getOwned().toString().c_str();
                //char *intStr = std::itoa(i);
                //string threadName(intStr);
                Client::initThread(ns.c_str());
                Lock::ParallelBatchWriterMode::iAmABatchParticipant();
               
                typedef map<BSONObj, vector<BSONObj> >::iterator it_type;
		for(it_type iterator = fromList.begin(); iterator!=fromList.end(); iterator++){
		    vector<BSONObj> ranges = iterator->second;
		    BSONObj from = iterator->first;
                    string fromStr = from["from"].str();

                    log() << "[WWT_SingleMigrate] start fetching data, Target: " << from.toString() << endl;

                      scoped_ptr<ScopedDbConnection> fromConn(ScopedDbConnection::getScopedDbConnection( fromStr ) );
                      int from_count = 0;
                      for(vector<BSONObj>::iterator rangeIt = ranges.begin(); rangeIt!=ranges.end(); rangeIt++)
		      {  
                           log() << "[WWT_SingleMigrate] " << (*rangeIt).toString() << endl;
                           BSONObj o;
			   BSONObj qRange = (*rangeIt)["range"].Obj().getOwned();

                           int range_count = 0;
                           //fetch remote data
			   while(1)
			   {
				log() << "Query Range:" << qRange.toString() << "from" << fromStr << endl;

				try
                		{
					scoped_ptr<DBClientCursor> cursor(fromConn->get()->query(ns, qRange, 0, 0, 0, QueryOption_SlaveOk)); 
                                	try
					{

						while (cursor->more()) {
							range_count++;
							o = cursor->next().getOwned();
							//log() << "[MYCODE] DATA: " << o.toString() << rsLog;
        						{
                                        
            						PageFaultRetryableSection pgrs;
	        	    				while ( 1 ) {
    	    	        					try {
									Lock::DBWrite r(ns);
									Client::Context context(ns);
									theDataFileMgr.insert(ns.c_str(), o.objdata(), o.objsize());
            	        						break;
            	    						}
            	    						catch ( PageFaultException& e ) {
            	        						e.touch();
            	    						}
            						}
        					}
					}
						break;
					}
					catch (DBException e)
					{
                                                log() << "Exception" << e.what() <<endl;
						log() << "Last BSONObj before crash:" << o.toString() << endl;

						BSONObjBuilder b;
						BSONObjBuilder sub(b.subobjStart(key));
						sub.appendAs(o[key], "$gt");
						BSONObj rangeVal = qRange[key].Obj();
						if (!rangeVal["$lt"].eoo())
							sub.append(rangeVal["$lt"]);
						BSONObj subObj = sub.done();
						qRange = b.done().getOwned();
					}
				}
                		catch (DBException e)
                		{
                	    		log() << "[MYCODE] DBClientCursor call failed" << endl;
               		 	}

			 }//end while(1)
                         from_count+=range_count;
                         log() << "[WWT] Fetch data in range " <<qRange.toString() << " count " << range_count  << endl; 
                         //remove remote data
                         while (true)
			 {
				try
				{
					fromConn->get()->remove(ns, qRange);
					break;
				}
				catch (DBException e)
				{
					continue;
				}
			}

			log() << "[MYCODE WWT] Removal Complete" << endl;

		      }//end for rangeIt
                      try
            	      {
			 fromConn->done();
            	      }
            	      catch(DBException e)
            	      {
              		log() << "[MYCODE] Caught exception while killing connection" << endl;
            	      }
                      log() << "[WWT] Fetched data Result: " << fromStr << " count " << from_count << endl;
                      totalCount +=from_count;
                }//end for fromIt
                log() << "[WWT_TIME] time for this threads" << "in " <<t.millis() << " count = " << totalCount <<endl;
                cc().shutdown();
            }
            
	}moveDataCmd;

    bool ShardingState::inCriticalMigrateSection() {
        return migrateFromStatus.getInCriticalSection();
    }

    bool ShardingState::waitTillNotInCriticalSection( int maxSecondsToWait ) {
        return migrateFromStatus.waitTillNotInCriticalSection( maxSecondsToWait );
    }

    /* -----
       below this are the "to" side commands

       command to initiate
       worker thread
         does initial clone
         pulls initial change set
         keeps pulling
         keeps state
       command to get state
       commend to "commit"
    */

    class MigrateStatus {
    public:
        
        MigrateStatus() : m_active("MigrateStatus") { active = false; }

        void prepare() {
            scoped_lock l(m_active); // reading and writing 'active'

            verify( ! active );
            state = READY;
            errmsg = "";

            numCloned = 0;
            clonedBytes = 0;
            numCatchup = 0;
            numSteady = 0;

            active = true;
        }

        void go() {
            try {
                _go();
            }
            catch ( std::exception& e ) {
                state = FAIL;
                errmsg = e.what();
                error() << "migrate failed: " << e.what() << migrateLog;
            }
            catch ( ... ) {
                state = FAIL;
                errmsg = "UNKNOWN ERROR";
                error() << "migrate failed with unknown exception" << migrateLog;
            }
            setActive( false );
        }

        void _go() {
            verify( getActive() );
            verify( state == READY );
            verify( ! min.isEmpty() );
            verify( ! max.isEmpty() );
            
            slaveCount = ( getSlaveCount() / 2 ) + 1;

            log() << "starting receiving-end of migration of chunk " << min << " -> " << max <<
                    " for collection " << ns << " from " << from <<
                    " (" << getSlaveCount() << " slaves detected)" << endl;

            string errmsg;
            MoveTimingHelper timing( "to" , ns , min , max , 5 /* steps */ , errmsg );

            scoped_ptr<ScopedDbConnection> connPtr(
                    ScopedDbConnection::getScopedDbConnection( from ) );
            ScopedDbConnection& conn = *connPtr;
            conn->getLastError(); // just test connection

            {
                // 0. copy system.namespaces entry if collection doesn't already exist
                Client::WriteContext ctx( ns );
                // Only copy if ns doesn't already exist
                if ( ! nsdetails( ns ) ) {
                    string system_namespaces = NamespaceString( ns ).db + ".system.namespaces";
                    BSONObj entry = conn->findOne( system_namespaces, BSON( "name" << ns ) );
                    if ( entry["options"].isABSONObj() ) {
                        string errmsg;
                        if ( ! userCreateNS( ns.c_str(), entry["options"].Obj(), errmsg, true, 0 ) )
                            warning() << "failed to create collection with options: " << errmsg
                                      << endl;
                    }
                }
            }

            {                
                // 1. copy indexes
                
                vector<BSONObj> all;
                {
                    auto_ptr<DBClientCursor> indexes = conn->getIndexes( ns );
                    
                    while ( indexes->more() ) {
                        all.push_back( indexes->next().getOwned() );
                    }
                }

                for ( unsigned i=0; i<all.size(); i++ ) {
                    BSONObj idx = all[i];
                    Client::WriteContext ct( ns );
                    string system_indexes = cc().database()->name + ".system.indexes";
                    theDataFileMgr.insertAndLog( system_indexes.c_str() , idx, true /* flag fromMigrate in oplog */ );
                }

                timing.done(1);
            }

            {

                BSONObj indexKeyPattern;
                if ( !findShardKeyIndexPattern_unlocked( ns, shardKeyPattern, &indexKeyPattern ) ) {
                    errmsg = "collection or index dropped during migrate";
                    warning() << errmsg << endl;
                    state = FAIL;
                    return;
                }

                // 2. delete any data already in range
                RemoveSaver rs( "moveChunk" , ns , "preCleanup" );
                long long num = Helpers::removeRange( ns,
                                                      min,
                                                      max,
                                                      indexKeyPattern,
                                                      false, /*maxInclusive*/
                                                      secondaryThrottle, /* secondaryThrottle */
                                                      cmdLine.moveParanoia ? &rs : 0, /*callback*/
                                                      true ); /* flag fromMigrate in oplog */
                if ( num )
                    warning() << "moveChunkCmd deleted data already in chunk # objects: " << num << migrateLog;

                timing.done(2);
            }


            {
                // 3. initial bulk clone
                state = CLONE;

                while ( true ) {
                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_migrateClone" << 1 ) , res ) ) {  // gets array of objects to copy, in disk order
                        state = FAIL;
                        errmsg = "_migrateClone failed: ";
                        errmsg += res.toString();
                        error() << errmsg << migrateLog;
                        conn.done();
                        return;
                    }

                    BSONObj arr = res["objects"].Obj();
                    int thisTime = 0;

                    BSONObjIterator i( arr );
                    while( i.more() ) {
                        BSONObj o = i.next().Obj();
                        {
                            PageFaultRetryableSection pgrs;
                            while ( 1 ) {
                                try {
                                    Lock::DBWrite lk( ns );
                                    Helpers::upsert( ns, o, true );
                                    break;
                                }
                                catch ( PageFaultException& e ) {
                                    e.touch();
                                }
                            }
                        }
                        thisTime++;
                        numCloned++;
                        clonedBytes += o.objsize();

                        if ( secondaryThrottle && thisTime > 0 ) {
                            if ( ! waitForReplication( cc().getLastOp(), 2, 60 /* seconds to wait */ ) ) {
                                warning() << "secondaryThrottle on, but doc insert timed out after 60 seconds, continuing" << endl;
                            }
                        }
                    }

                    if ( thisTime == 0 )
                        break;
                }

                timing.done(3);
            }

            // if running on a replicated system, we'll need to flush the docs we cloned to the secondaries
            ReplTime lastOpApplied = cc().getLastOp().asDate();

            {
                // 4. do bulk of mods
                state = CATCHUP;
                while ( true ) {
                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_transferMods" << 1 ) , res ) ) {
                        state = FAIL;
                        errmsg = "_transferMods failed: ";
                        errmsg += res.toString();
                        error() << "_transferMods failed: " << res << migrateLog;
                        conn.done();
                        return;
                    }
                    if ( res["size"].number() == 0 )
                        break;

                    apply( res , &lastOpApplied );
                    
                    const int maxIterations = 3600*50;
                    int i;
                    for ( i=0;i<maxIterations; i++) {
                        if ( state == ABORT ) {
                            timing.note( "aborted" );
                            return;
                        }
                        
                        if ( opReplicatedEnough( lastOpApplied ) )
                            break;
                        
                        if ( i > 100 ) {
                            warning() << "secondaries having hard time keeping up with migrate" << migrateLog;
                        }

                        sleepmillis( 20 );
                    }

                    if ( i == maxIterations ) {
                        errmsg = "secondary can't keep up with migrate";
                        error() << errmsg << migrateLog;
                        conn.done();
                        state = FAIL;
                        return;
                    } 
                }

                timing.done(4);
            }

            { 
                // pause to wait for replication
                // this will prevent us from going into critical section until we're ready
                Timer t;
                while ( t.minutes() < 600 ) {
                    log() << "Waiting for replication to catch up before entering critical section"
                          << endl;
                    if ( flushPendingWrites( lastOpApplied ) )
                        break;
                    sleepsecs(1);
                }
            }

            {
                // 5. wait for commit

                state = STEADY;
                while ( state == STEADY || state == COMMIT_START ) {
                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_transferMods" << 1 ) , res ) ) {
                        log() << "_transferMods failed in STEADY state: " << res << migrateLog;
                        errmsg = res.toString();
                        state = FAIL;
                        conn.done();
                        return;
                    }

                    if ( res["size"].number() > 0 && apply( res , &lastOpApplied ) )
                        continue;

                    if ( state == ABORT ) {
                        timing.note( "aborted" );
                        return;
                    }
                    
                    if ( state == COMMIT_START ) {
                        if ( flushPendingWrites( lastOpApplied ) )
                            break;
                    }
                    
                    sleepmillis( 10 );
                }

                if ( state == FAIL ) {
                    errmsg = "timed out waiting for commit";
                    return;
                }

                timing.done(5);
            }

            state = DONE;
            conn.done();
        }

        void status( BSONObjBuilder& b ) {
            b.appendBool( "active" , getActive() );

            b.append( "ns" , ns );
            b.append( "from" , from );
            b.append( "min" , min );
            b.append( "max" , max );
            b.append( "shardKeyPattern" , shardKeyPattern );

            b.append( "state" , stateString() );
            if ( state == FAIL )
                b.append( "errmsg" , errmsg );
            {
                BSONObjBuilder bb( b.subobjStart( "counts" ) );
                bb.append( "cloned" , numCloned );
                bb.append( "clonedBytes" , clonedBytes );
                bb.append( "catchup" , numCatchup );
                bb.append( "steady" , numSteady );
                bb.done();
            }


        }

        bool apply( const BSONObj& xfer , ReplTime* lastOpApplied ) {
            ReplTime dummy;
            if ( lastOpApplied == NULL ) {
                lastOpApplied = &dummy;
            }

            bool didAnything = false;

            if ( xfer["deleted"].isABSONObj() ) {
                RemoveSaver rs( "moveChunk" , ns , "removedDuring" );

                BSONObjIterator i( xfer["deleted"].Obj() );
                while ( i.more() ) {
                    Client::WriteContext cx(ns);

                    BSONObj id = i.next().Obj();

                    // do not apply deletes if they do not belong to the chunk being migrated
                    BSONObj fullObj;
                    if ( Helpers::findById( cc() , ns.c_str() , id, fullObj ) ) {
                        if ( ! isInRange( fullObj , min , max , shardKeyPattern ) ) {
                            log() << "not applying out of range deletion: " << fullObj << migrateLog;

                            continue;
                        }
                    }

                    // id object most likely has form { _id : ObjectId(...) }
                    // infer from that correct index to use, e.g. { _id : 1 }
                    BSONObj idIndexPattern = Helpers::inferKeyPattern( id );

                    // TODO: create a better interface to remove objects directly
                    Helpers::removeRange( ns ,
                                          id ,
                                          id,
                                          idIndexPattern ,
                                          true , /*maxInclusive*/
                                          false , /* secondaryThrottle */
                                          cmdLine.moveParanoia ? &rs : 0 , /*callback*/
                                          true ); /*fromMigrate*/

                    *lastOpApplied = cx.ctx().getClient()->getLastOp().asDate();
                    didAnything = true;
                }
            }

            if ( xfer["reload"].isABSONObj() ) {
                BSONObjIterator i( xfer["reload"].Obj() );
                while ( i.more() ) {
                    Client::WriteContext cx(ns);

                    BSONObj it = i.next().Obj();

                    Helpers::upsert( ns , it , true );

                    *lastOpApplied = cx.ctx().getClient()->getLastOp().asDate();
                    didAnything = true;
                }
            }

            return didAnything;
        }

        bool opReplicatedEnough( const ReplTime& lastOpApplied ) {
            // if replication is on, try to force enough secondaries to catch up
            // TODO opReplicatedEnough should eventually honor priorities and geo-awareness
            //      for now, we try to replicate to a sensible number of secondaries
            return mongo::opReplicatedEnough( lastOpApplied , slaveCount );
        }

        bool flushPendingWrites( const ReplTime& lastOpApplied ) {
            if ( ! opReplicatedEnough( lastOpApplied ) ) {
                OpTime op( lastOpApplied );
                OCCASIONALLY warning() << "migrate commit waiting for " << slaveCount 
                                       << " slaves for '" << ns << "' " << min << " -> " << max 
                                       << " waiting for: " << op
                                       << migrateLog;
                return false;
            }

            log() << "migrate commit succeeded flushing to secondaries for '" << ns << "' " << min << " -> " << max << migrateLog;

            {
                Lock::GlobalRead lk;

                // if durability is on, force a write to journal
                if ( getDur().commitNow() ) {
                    log() << "migrate commit flushed to journal for '" << ns << "' " << min << " -> " << max << migrateLog;
                }
            }

            return true;
        }

        string stateString() {
            switch ( state ) {
            case READY: return "ready";
            case CLONE: return "clone";
            case CATCHUP: return "catchup";
            case STEADY: return "steady";
            case COMMIT_START: return "commitStart";
            case DONE: return "done";
            case FAIL: return "fail";
            case ABORT: return "abort";
            }
            verify(0);
            return "";
        }

        bool startCommit() {
            if ( state != STEADY )
                return false;
            state = COMMIT_START;
            
            Timer t;
            // we wait for the commit to succeed before giving up
            while ( t.seconds() <= 30 ) {
                log() << "Waiting for commit to finish" << endl;
                sleepmillis(1);
                if ( state == DONE )
                    return true;
            }
            state = FAIL;
            log() << "startCommit never finished!" << migrateLog;
            return false;
        }

        void abort() {
            state = ABORT;
            errmsg = "aborted";
        }

        bool getActive() const { scoped_lock l(m_active); return active; }
        void setActive( bool b ) { scoped_lock l(m_active); active = b; }

        mutable mongo::mutex m_active;
        bool active;

        string ns;
        string from;

        BSONObj min;
        BSONObj max;
        BSONObj shardKeyPattern;

        long long numCloned;
        long long clonedBytes;
        long long numCatchup;
        long long numSteady;
        bool secondaryThrottle;

        int slaveCount;

        enum State { READY , CLONE , CATCHUP , STEADY , COMMIT_START , DONE , FAIL , ABORT } state;
        string errmsg;

    } migrateStatus;

    void migrateThread() {
        Client::initThread( "migrateThread" );
        if (!noauth) {
            ShardedConnectionInfo::addHook();
            cc().getAuthorizationManager()->grantInternalAuthorization("_migrateThread");
        }
        migrateStatus.go();
        cc().shutdown();
    }

    class RecvChunkStartCommand : public ChunkCommandHelper {
    public:
        RecvChunkStartCommand() : ChunkCommandHelper( "_recvChunkStart" ) {}

        virtual LockType locktype() const { return WRITE; }  // this is so don't have to do locking internally
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkStart);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {

            if ( migrateStatus.getActive() ) {
                errmsg = "migrate already in progress";
                return false;
            }
            
            if ( OldDataCleanup::_numThreads > 0 ) {
                errmsg = 
                    str::stream() 
                    << "still waiting for a previous migrates data to get cleaned, can't accept new chunks, num threads: " 
                    << OldDataCleanup::_numThreads;
                return false;
            }

            if ( ! configServer.ok() )
                configServer.init( cmdObj["configServer"].String() );

            migrateStatus.prepare();

            migrateStatus.ns = cmdObj.firstElement().String();
            migrateStatus.from = cmdObj["from"].String();
            migrateStatus.min = cmdObj["min"].Obj().getOwned();
            migrateStatus.max = cmdObj["max"].Obj().getOwned();
            migrateStatus.secondaryThrottle = cmdObj["secondaryThrottle"].trueValue();
            if (cmdObj.hasField("shardKeyPattern")) {
                migrateStatus.shardKeyPattern = cmdObj["shardKeyPattern"].Obj().getOwned();
            } else {
                // shardKeyPattern may not be provided if another shard is from pre 2.2
                // In that case, assume the shard key pattern is the same as the range
                // specifiers provided.
                BSONObj keya = Helpers::inferKeyPattern( migrateStatus.min );
                BSONObj keyb = Helpers::inferKeyPattern( migrateStatus.max );
                verify( keya == keyb );

                warning() << "No shard key pattern provided by source shard for migration."
                    " This is likely because the source shard is running a version prior to 2.2."
                    " Falling back to assuming the shard key matches the pattern of the min and max"
                    " chunk range specifiers.  Inferred shard key: " << keya << endl;

                migrateStatus.shardKeyPattern = keya.getOwned();
            }

            if ( migrateStatus.secondaryThrottle && ! anyReplEnabled() ) {
                warning() << "secondaryThrottle asked for, but not replication" << endl;
                migrateStatus.secondaryThrottle = false;
            }

            boost::thread m( migrateThread );

            result.appendBool( "started" , true );
            return true;
        }

    } recvChunkStartCmd;

    class RecvChunkStatusCommand : public ChunkCommandHelper {
    public:
        RecvChunkStatusCommand() : ChunkCommandHelper( "_recvChunkStatus" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkStatus);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            migrateStatus.status( result );
            return 1;
        }

    } recvChunkStatusCommand;

    class RecvChunkCommitCommand : public ChunkCommandHelper {
    public:
        RecvChunkCommitCommand() : ChunkCommandHelper( "_recvChunkCommit" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkCommit);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            bool ok = migrateStatus.startCommit();
            migrateStatus.status( result );
            return ok;
        }

    } recvChunkCommitCommand;

    class RecvChunkAbortCommand : public ChunkCommandHelper {
    public:
        RecvChunkAbortCommand() : ChunkCommandHelper( "_recvChunkAbort" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::_recvChunkAbort);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            migrateStatus.abort();
            migrateStatus.status( result );
            return true;
        }

    } recvChunkAboortCommand;


    class IsInRangeTest : public StartupTest {
    public:
        void run() {
            BSONObj min = BSON( "x" << 1 );
            BSONObj max = BSON( "x" << 5 );
            BSONObj skey = BSON( "x" << 1 );

            verify( ! isInRange( BSON( "x" << 0 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 1 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 3 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 4 ) , min , max , skey ) );
            verify( ! isInRange( BSON( "x" << 5 ) , min , max , skey ) );
            verify( ! isInRange( BSON( "x" << 6 ) , min , max , skey ) );

            BSONObj obj = BSON( "n" << 3 );
            BSONObj min2 = BSON( "x" << BSONElementHasher::hash64( obj.firstElement() , 0 ) - 2 );
            BSONObj max2 = BSON( "x" << BSONElementHasher::hash64( obj.firstElement() , 0 ) + 2 );
            BSONObj hashedKey =  BSON( "x" << "hashed" );

            verify( isInRange( BSON( "x" << 3 ) , min2 , max2 , hashedKey ) );
            verify( ! isInRange( BSON( "x" << 3 ) , min , max , hashedKey ) );
            verify( ! isInRange( BSON( "x" << 4 ) , min2 , max2 , hashedKey ) );

            LOG(1) << "isInRangeTest passed" << migrateLog;
        }
    } isInRangeTest;
}
