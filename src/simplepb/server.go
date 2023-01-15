package simplepb

//
// This is a outline of primary-backup replication based on a simplifed version of Viewstamp replication.
//
//
//

import (
	"fmt"
	"sync"

	"labrpc"
)

// the 3 possible server status
const (
	NORMAL = iota
	VIEWCHANGE
	RECOVERING
)

// PBServer defines the state of a replica server (either primary or backup)
type PBServer struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	me             int                 // this peer's index into peers[]
	currentView    int                 // what this peer believes to be the current active view
	status         int                 // the server's current status (NORMAL, VIEWCHANGE or RECOVERING)
	lastNormalView int                 // the latest view which had a NORMAL status

	log         []interface{} // the log of "commands"
	commitIndex int           // all log entries <= commitIndex are considered to have been committed.

	// ... other state that you might need ...
}

// Prepare defines the arguments for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC args struct
type PrepareArgs struct {
	View          int         // the primary's current view
	PrimaryCommit int         // the primary's commitIndex
	Index         int         // the index position at which the log entry is to be replicated on backups
	Entry         interface{} // the log entry to be replicated
}

// PrepareReply defines the reply for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC reply struct
type PrepareReply struct {
	View    int  // the backup's current view
	Success bool // whether the Prepare request has been accepted or rejected
}

// RecoverArgs defined the arguments for the Recovery RPC
type RecoveryArgs struct {
	View   int // the view that the backup would like to synchronize with
	Server int // the server sending the Recovery RPC (for debugging)
}

type RecoveryReply struct {
	View          int           // the view of the primary
	Entries       []interface{} // the primary's log including entries replicated up to and including the view.
	PrimaryCommit int           // the primary's commitIndex
	Success       bool          // whether the Recovery request has been accepted or rejected
}

type ViewChangeArgs struct {
	View int // the new view to be changed into
}

type ViewChangeReply struct {
	LastNormalView int           // the latest view which had a NORMAL status at the server
	Log            []interface{} // the log at the server
	Success        bool          // whether the ViewChange request has been accepted/rejected
}

type StartViewArgs struct {
	View int           // the new view which has completed view-change
	Log  []interface{} // the log associated with the new new
}

type StartViewReply struct {
}

// GetPrimary is an auxilary function that returns the server index of the
// primary server given the view number (and the total number of replica servers)
func GetPrimary(view int, nservers int) int {
	return view % nservers
}

// IsCommitted is called by tester to check whether an index position
// has been considered committed by this server
func (srv *PBServer) IsCommitted(index int) (committed bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.commitIndex >= index {
		return true
	}
	return false
}

// ViewStatus is called by tester to find out the current view of this server
// and whether this view has a status of NORMAL.
func (srv *PBServer) ViewStatus() (currentView int, statusIsNormal bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.currentView, srv.status == NORMAL
}

// GetEntryAtIndex is called by tester to return the command replicated at
// a specific log index. If the server's log is shorter than "index", then
// ok = false, otherwise, ok = true
func (srv *PBServer) GetEntryAtIndex(index int) (ok bool, command interface{}) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.log) > index {
		return true, srv.log[index]
	}
	return false, command
}

// Kill is called by tester to clean up (e.g. stop the current server)
// before moving on to the next test
func (srv *PBServer) Kill() {
	// Your code here, if necessary
}

// Make is called by tester to create and initalize a PBServer
// peers is the list of RPC endpoints to every server (including self)
// me is this server's index into peers.
// startingView is the initial view (set to be zero) that all servers start in
func Make(peers []*labrpc.ClientEnd, me int, startingView int) *PBServer {
	srv := &PBServer{
		peers:          peers,
		me:             me,
		currentView:    startingView,
		lastNormalView: startingView,
		status:         NORMAL,
	}
	// all servers' log are initialized with a dummy command at index 0
	var v interface{}
	srv.log = append(srv.log, v)

	// Your other initialization code here, if there's any
	return srv
}

// Start() is invoked by tester on some replica server to replicate a
// command.  Only the primary should process this request by appending
// the command to its log and then return *immediately* (while the log is being replicated to backup servers).
// if this server isn't the primary, returns false.
// Note that since the function returns immediately, there is no guarantee that this command
// will ever be committed upon return, since the primary
// may subsequently fail before replicating the command to all servers
//
// The first return value is the index that the command will appear at
// *if it's eventually committed*. The second return value is the current
// view. The third return value is true if this server believes it is
// the primary.
//
// When Start(command) is invoked, the primary should append the command in its log
// and then send Prepare RPCs to other servers to instruct them to replicate the command in the same index in their log.
// Note that a server should do the processing for Start only if it believes itself to be the current primary and that its status is NORMAL (as opposed to VIEW-CHANGE or RECOVERY).
func (srv *PBServer) Start(command interface{}) (
	index int, view int, ok bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	// do not process command if status is not NORMAL
	// and if i am not the primary in the current view
	if srv.status != NORMAL {
		return -1, srv.currentView, false
	} else if GetPrimary(srv.currentView, len(srv.peers)) != srv.me {
		return -1, srv.currentView, false
	}

	fmt.Printf("Server{%d} View{%d} Replicating command=%d\n", srv.me, srv.currentView, command)

	view = srv.currentView
	index = len(srv.log)
	srv.log = append(srv.log, command)

	// send StartView to all servers including myself
	prepareChan := make(chan *PrepareReply, len(srv.peers))
	for i := 0; i < len(srv.peers); i++ {
		if i != srv.me {
			go func(server int) {
				prepArgs := &PrepareArgs{
					View:          view,            // the primary's current view
					PrimaryCommit: srv.commitIndex, // the primary's commitIndex
					Index:         index,           // the index position at which the log entry is to be replicated on backups
					Entry:         command,         // the log entry to be replicated
				}
				var prepReply PrepareReply
				ok := srv.sendPrepare(server, prepArgs, &prepReply)
				if ok {
					prepareChan <- &prepReply
				} else {
					prepareChan <- nil
				}
			}(i)
		}
	}

	// Consider the index "committed" once received Success=true responses from a majority of servers (including itself)
	go func() {
		// fmt.Printf("Server{%d} Checking\n", srv.me)
		var successReplies []*PrepareReply
		var nReplies int
		majority := len(srv.peers) / 2
		for r := range prepareChan {
			nReplies++
			if r != nil && r.Success {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(srv.peers) || len(successReplies) == majority {
				if index > srv.commitIndex {
					srv.commitIndex = index
					fmt.Printf("Server{%d} committed %d\n", srv.me, srv.commitIndex)
					break
				}
			}
		}
		// fmt.Printf("Server{%d} Done checking\n", srv.me)
	}()

	ok = true
	return index, view, ok
}

// exmple code to send an AppendEntries RPC to a server.
// server is the index of the target server in srv.peers[].
// expects RPC arguments in args.
// The RPC library fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
func (srv *PBServer) sendPrepare(server int, args *PrepareArgs, reply *PrepareReply) bool {
	ok := srv.peers[server].Call("PBServer.Prepare", args, reply)
	return ok
}

// Prepare is the RPC handler for the Prepare RPC
// Upon receiving a Prepare RPC message, the backup checks whether the message's view and its currentView match
// and whether the next entry to be added to the log is indeed at the index specified in the message.
// If so, the backup adds the message's entry to the log and replies Success=ok.
// Otherwise, the backup replies Success=false.
// Furthermore, if the backup's state falls behind the primary (e.g. its view is smaller or its log is missing entries),
// it performs recovery to transfer the primary's log.
// Note that the backup server needs to process Prepare messages according to their index order, otherwise, it would end up unnecessarily rejecting many messages.
func (srv *PBServer) Prepare(args *PrepareArgs, reply *PrepareReply) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.currentView > args.View {
		// Simply reject requests from outdated (primary) server
		reply.Success = false
		return
	}

	if srv.currentView < args.View {
		// View change recovervy
		recoveryArgs := &RecoveryArgs{
			View:   srv.currentView,
			Server: srv.me,
		}
		var recoveryReply RecoveryReply
		primary := GetPrimary(args.View, len(srv.peers))
		ok := srv.peers[primary].Call("PBServer.Recovery", recoveryArgs, &recoveryReply)
		if !ok {
			reply.Success = false
			return
		}
		srv.currentView = recoveryReply.View
		srv.log = recoveryReply.Entries
		srv.commitIndex = args.PrimaryCommit
		srv.lastNormalView = recoveryArgs.View
		fmt.Printf("Recovered currentView=%d, commitIdx=%d, log=%d, stat=%v\n", srv.currentView, srv.commitIndex, srv.log, srv.status)
		return
	}

	if args.Index < len(srv.log) {
		reply.Success = true
		return
	}
	if args.Index > len(srv.log) {
		recoveryArgs := &RecoveryArgs{
			View:   srv.currentView,
			Server: srv.me,
		}
		var recoveryReply RecoveryReply
		primary := GetPrimary(srv.currentView, len(srv.peers))
		ok := srv.peers[primary].Call("PBServer.Recovery", recoveryArgs, &recoveryReply)
		if !ok {
			reply.Success = false
			return
		}
		srv.currentView = recoveryReply.View
		for missedIdx := len(srv.log); missedIdx < len(recoveryReply.Entries) && missedIdx < args.Index; missedIdx++ {
			srv.log = append(srv.log, recoveryReply.Entries[missedIdx])
		}
	}
	srv.log = append(srv.log, args.Entry)
	srv.commitIndex = args.PrimaryCommit
	reply.Success = true
	fmt.Printf("Server{%d} commitIdx=%d. Processed %d at pos=%d. Curr log=%d\n", srv.me, srv.commitIndex, args.Entry, args.Index, srv.log)
}

// Recovery is the RPC handler for the Recovery RPC
func (srv *PBServer) Recovery(args *RecoveryArgs, reply *RecoveryReply) {
	// Handled by Primary
	reply.Entries = srv.log
	reply.PrimaryCommit = srv.commitIndex
	reply.View = srv.currentView
	reply.Success = true
}

// Some external oracle prompts the primary of the newView to
// switch to the newView.
// PromptViewChange just kicks start the view change protocol to move to the newView
// It does not block waiting for the view change process to complete.
func (srv *PBServer) PromptViewChange(newView int) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	newPrimary := GetPrimary(newView, len(srv.peers))

	if newPrimary != srv.me { //only primary of newView should do view change
		return
	} else if newView <= srv.currentView {
		return
	}
	vcArgs := &ViewChangeArgs{
		View: newView,
	}
	vcReplyChan := make(chan *ViewChangeReply, len(srv.peers))
	// send ViewChange to all servers including myself
	for i := 0; i < len(srv.peers); i++ {
		go func(server int) {
			var reply ViewChangeReply
			ok := srv.peers[server].Call("PBServer.ViewChange", vcArgs, &reply)
			// fmt.Printf("node-%d (nReplies %d) received reply ok=%v reply=%v\n", srv.me, nReplies, ok, r.reply)
			if ok {
				vcReplyChan <- &reply
			} else {
				vcReplyChan <- nil
			}
		}(i)
	}

	// wait to receive ViewChange replies
	// if view change succeeds, send StartView RPC
	go func() {
		var successReplies []*ViewChangeReply
		var nReplies int
		majority := len(srv.peers)/2 + 1
		for r := range vcReplyChan {
			nReplies++
			if r != nil && r.Success {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(srv.peers) || len(successReplies) == majority {
				break
			}
		}
		ok, log := srv.determineNewViewLog(successReplies)
		if !ok {
			return
		}
		svArgs := &StartViewArgs{
			View: vcArgs.View,
			Log:  log,
		}
		// send StartView to all servers including myself
		for i := 0; i < len(srv.peers); i++ {
			var reply StartViewReply
			go func(server int) {
				// fmt.Printf("node-%d sending StartView v=%d to node-%d\n", srv.me, svArgs.View, server)
				srv.peers[server].Call("PBServer.StartView", svArgs, &reply)
			}(i)
		}
	}()
}

// determineNewViewLog is invoked to determine the log for the newView based on
// the collection of replies for successful ViewChange requests.
// if a quorum of successful replies exist, then ok is set to true.
// otherwise, ok = false.
func (srv *PBServer) determineNewViewLog(successReplies []*ViewChangeReply) (
	ok bool, newViewLog []interface{}) {
	// chooses the log among the majority of successful replies using this rule:
	// it picks the log whose lastest normal view number is the largest.
	// If there are more than one such logs, it picks the longest log among those.
	majority := len(srv.peers)/2 + 1
	if len(successReplies) < majority {
		return false, newViewLog
	}

	lastNormalView := 0
	for i := 0; i < len(successReplies); i++ {
		var reply = successReplies[i]
		if reply.LastNormalView > lastNormalView {
			newViewLog = reply.Log
			lastNormalView = reply.LastNormalView
		} else if reply.LastNormalView == lastNormalView && len(reply.Log) > len(newViewLog) {
			newViewLog = reply.Log
		}
	}
	ok = true

	return ok, newViewLog
}

// ViewChange is the RPC handler to process ViewChange RPC.
func (srv *PBServer) ViewChange(args *ViewChangeArgs, reply *ViewChangeReply) {
	// Upon receving ViewChange, a replica server checks that the view number included in the message is
	// indeed larger than what it thinks the current view number is.
	// If the check succeeds, it sets its current view number to that in the message and modifies its status to VIEW-CHANGE.
	// It replies Success=true and includes its current log (in its entirety) as well as the latest view-number that has been considered NORMAL. If the check fails, the backup replies Success=false.

	srv.mu.Lock()
	defer srv.mu.Unlock()

	fmt.Printf("Server{%d} current view=%d, lastNormalView=%d - ViewChange to %d, log=%d\n", srv.me, srv.currentView, srv.lastNormalView, args.View, srv.log)

	if args.View <= srv.currentView {
		reply.Success = false
		return
	}

	srv.currentView = args.View
	srv.status = VIEWCHANGE

	reply.Success = true
	reply.LastNormalView = srv.lastNormalView
	reply.Log = srv.log
}

// StartView is the RPC handler to process StartView RPC.
func (srv *PBServer) StartView(args *StartViewArgs, reply *StartViewReply) {
	// Upon receive StartView, a server sets the new-view as indicated in the message and changes its status to be NORMAL.
	// Note that before setting the new-view according to the StartView RPC message, the server must again check that its current view is
	// no bigger than that in the RPC message, which would mean that there's been no concurrent view-change for a larger view.

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if args.View < srv.currentView {
		return
	}

	srv.currentView = args.View
	srv.log = args.Log
	srv.status = NORMAL
	srv.lastNormalView = srv.currentView
	srv.commitIndex = len(srv.log) - 1
	fmt.Printf("Server{%d} current view=%d, lastNormalView=%d - StartView %d, log=%d, commitIdx=%d, me=%d\n", srv.me, srv.currentView, srv.lastNormalView, args.View, srv.log, srv.commitIndex, srv.me)
}
