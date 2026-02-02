import asyncio
import json
import time
import random
from typing import List, Dict, Any, Optional, Tuple

class RaftNode:
    """
    Raft Consensus Module.
    Manages state (Leader/Follower/Candidate), Log Replication, and Leader Election.
    """
    def __init__(self, node_id: str, peers: List[str], server, self_address: str = None):
        self.node_id = node_id
        self.peers = peers # List of "host:port" strings
        self.server = server # Reference to AsyncTCPServer for sending callback
        self.self_address = self_address  # Own host:port for redirects
        
        # Persistent State
        self.current_term = 0
        self.voted_for = None
        self.log: List[Dict[str, Any]] = [] # List of {"term": t, "command": cmd}
        
        # Volatile State
        self.commit_index = -1
        self.last_applied = -1
        self.role = "FOLLOWER" # FOLLOWER, CANDIDATE, LEADER
        self.leader_id = None
        
        # Leader Volatile State
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Election Timer
        self.election_timeout_task = None
        self.heartbeat_task = None
        
        # Constants
        self.HEARTBEAT_INTERVAL = 0.5 # 500ms
        self.ELECTION_TIMEOUT_MIN = 1.5 # 1.5s
        self.ELECTION_TIMEOUT_MAX = 3.0 # 3.0s

    async def start(self):
        print(f"[{self.node_id}] Raft Node Started. Term: {self.current_term}")
        
        # Standalone mode: auto-become leader if no peers
        if not self.peers:
            print(f"[{self.node_id}] No peers, becoming standalone leader")
            self.role = "LEADER"
            self.leader_id = self.self_address
            self.current_term = 1
        else:
            self.reset_election_timer()

    async def stop(self):
        if self.election_timeout_task:
            self.election_timeout_task.cancel()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()

    def reset_election_timer(self):
        if self.election_timeout_task:
            self.election_timeout_task.cancel()
        
        timeout = random.uniform(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)
        self.election_timeout_task = asyncio.create_task(self._election_timeout(timeout))

    async def _election_timeout(self, timeout):
        try:
            await asyncio.sleep(timeout)
            # Timeout triggered -> Start Election
            await self.start_election()
        except asyncio.CancelledError:
            pass

    async def start_election(self):
        print(f"[{self.node_id}] Election Timeout. Starting Election.")
        self.role = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        votes_received = 1 # Vote for self
        
        # Request Votes from all peers
        last_log_idx = len(self.log) - 1
        last_log_term = self.log[last_log_idx]["term"] if self.log else 0
        
        request = {
            "type": "RequestVote",
            "term": self.current_term,
            "candidateId": self.node_id,
            "lastLogIndex": last_log_idx,
            "lastLogTerm": last_log_term
        }
        
        # Broadcast RequestVote
        futures = []
        for peer in self.peers:
            futures.append(self.server.send_rpc(peer, request))
            
        results = await asyncio.gather(*futures, return_exceptions=True)
        
        for i, res in enumerate(results):
            if isinstance(res, dict):
                if res.get("term", 0) > self.current_term:
                    self.become_follower(res["term"])
                    return
                if res.get("voteGranted"):
                    votes_received += 1
        
        print(f"[{self.node_id}] Votes received: {votes_received}/{len(self.peers) + 1}")
        
        # Check quorum
        if self.role == "CANDIDATE" and votes_received > (len(self.peers) + 1) / 2:
            await self.become_leader()
        else:
            # Election failed, reset timer for next attempt
            self.reset_election_timer()

    def become_follower(self, term):
        print(f"[{self.node_id}] Becoming Follower. Term: {term}")
        self.current_term = term
        self.role = "FOLLOWER"
        self.voted_for = None
        self.reset_election_timer()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()

    async def become_leader(self):
        print(f"[{self.node_id}] Becoming LEADER. Term: {self.current_term}")
        self.role = "LEADER"
        self.leader_id = self.self_address  # Store address for redirects
        if self.election_timeout_task:
            self.election_timeout_task.cancel()
            
        # Initialize Leader State
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = -1
            
        # Start Heartbeats
        self.heartbeat_task = asyncio.create_task(self._send_heartbeats())

    async def _send_heartbeats(self):
        while self.role == "LEADER":
            for peer in self.peers:
                asyncio.create_task(self.send_append_entries(peer))
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)

    async def send_append_entries(self, peer):
        prev_log_index = self.next_index[peer] - 1
        prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0
        entries = self.log[self.next_index[peer]:]
        
        request = {
            "type": "AppendEntries",
            "term": self.current_term,
            "leaderId": self.node_id,
            "leaderAddress": self.self_address,
            "prevLogIndex": prev_log_index,
            "prevLogTerm": prev_log_term,
            "entries": entries,
            "leaderCommit": self.commit_index
        }
        
        try:
            res = await self.server.send_rpc(peer, request)
            if not res: return # Failed
            
            if res.get("term", 0) > self.current_term:
                self.become_follower(res["term"])
                return
                
            if res.get("success"):
                # Update match_index and next_index
                if entries:
                    self.match_index[peer] = prev_log_index + len(entries)
                    self.next_index[peer] = self.match_index[peer] + 1
                    
                # Update Commit Index with quorum check
                if self.log:
                    match_indices = sorted(list(self.match_index.values()) + [len(self.log) - 1], reverse=True)
                    quorum_index = match_indices[(len(self.peers) + 1) // 2]
                    if quorum_index >= 0 and quorum_index > self.commit_index:
                        if self.log[quorum_index]["term"] == self.current_term:
                            print(f"[{self.node_id}] Advancing commit_index to {quorum_index}")
                            self.commit_index = quorum_index
                            await self.apply_logs()
                            # Send immediate heartbeats to propagate commit
                            for p in self.peers:
                                asyncio.create_task(self.send_append_entries(p))
                    
            else:
                # Backtrack
                self.next_index[peer] = max(0, self.next_index[peer] - 1)
                # Retry immediately? No, wait next heartbeat (simple)
                
        except Exception as e:
            # print(f"RPC Error to {peer}: {e}")
            pass

    async def handle_rpc(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Handle incoming Raft RPC"""
        msg_type = msg.get("type")
        term = msg.get("term", 0)
        
        if term > self.current_term:
            self.become_follower(term)
            
        reply = {"term": self.current_term}
        
        if msg_type == "RequestVote":
            return self._handle_request_vote(msg, reply)
        elif msg_type == "AppendEntries":
            return await self._handle_append_entries(msg, reply)
        return reply

    def _handle_request_vote(self, msg, reply):
        candidate_id = msg.get("candidateId")
        last_log_idx = msg.get("lastLogIndex")
        last_log_term = msg.get("lastLogTerm")
        
        vote_granted = False
        
        if msg["term"] < self.current_term:
            vote_granted = False
        elif (self.voted_for is None or self.voted_for == candidate_id):
            # Check log updateness
            my_last_idx = len(self.log) - 1
            my_last_term = self.log[my_last_idx]["term"] if self.log else 0
            
            if (last_log_term > my_last_term) or (last_log_term == my_last_term and last_log_idx >= my_last_idx):
                vote_granted = True
                self.voted_for = candidate_id
                self.reset_election_timer() # Granting vote resets timer
                
        reply["voteGranted"] = vote_granted
        return reply

    async def _handle_append_entries(self, msg, reply):
        leader_id = msg.get("leaderId")
        leader_address = msg.get("leaderAddress")
        prev_log_index = msg.get("prevLogIndex")
        prev_log_term = msg.get("prevLogTerm")
        entries = msg.get("entries")
        leader_commit = msg.get("leaderCommit")
        
        if msg["term"] < self.current_term:
            reply["success"] = False
            return reply
            
        self.reset_election_timer() # Valid heartbeat
        self.leader_id = leader_address  # Use address for client redirects
        
        # Check Log Consistency
        if prev_log_index >= len(self.log):
             reply["success"] = False
             return reply
             
        if prev_log_index >= 0 and self.log[prev_log_index]["term"] != prev_log_term:
             # Conflict
             # Delete conflict and all that follow
             self.log = self.log[:prev_log_index]
             reply["success"] = False
             return reply
             
        # Append Entries
        if entries:
            # In a real implementation we carefully merge. 
            # Here assuming simplified Append: replace/extend from prev_log_index + 1
            idx = prev_log_index + 1
            for entry in entries:
                if idx < len(self.log):
                    if self.log[idx]["term"] != entry["term"]:
                        self.log = self.log[:idx] # Trim conflict
                        self.log.append(entry)
                else:
                    self.log.append(entry)
                idx += 1
                
        reply["success"] = True
        
        # Commit
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            await self.apply_logs()
            
        return reply

    async def apply_logs(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            cmd = entry["command"]
            print(f"[{self.node_id}] Applying Log {self.last_applied}: {cmd}")
            # Call Server to apply to KV Store
            await self.server.apply_to_fsm(cmd)

    async def propose_command(self, command: Dict[str, Any]) -> bool:
        """Called by Server when client sends a write."""
        if self.role != "LEADER":
            return False
            
        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)
        target_index = len(self.log) - 1
        
        # Standalone mode: immediate commit (single node = quorum)
        if not self.peers:
            self.commit_index = target_index
            await self.apply_logs()
            return True
        
        # Trigger immediate replication attempt
        for peer in self.peers:
            asyncio.create_task(self.send_append_entries(peer))
            
        start_time = time.time()
        while self.commit_index < target_index:
            if time.time() - start_time > 2.0: # Timeout
                return False
            await asyncio.sleep(0.01)
        
        return True
