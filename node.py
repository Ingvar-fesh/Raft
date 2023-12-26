import threading
import time
import random
import socket
import pickle
import sys
import socketserver

TTL = 7


class Node:
    def __init__(self, id, adresses):
        self.random = random.Random(id)
        self.hashTable = {}
        self.state = "FOLLOWER"
        self.leaderId = None
        self.id = id
        self.leaderAlive = False
        self.myAddress = adresses.pop(id)
        print(self.myAddress)
        self.otherNodes = adresses
        self.currentTerm = 0
        self.votedFor = None
        self.log = ['init']
        self.logTerms = [0]
        self.commitIndex = 0
        self._last_applied = 0
        self._next_index = {}
        self._match_index = {}
        self._lock = threading.Lock()
        self._election_deadline = 0
        self._thread = threading.Thread(target=self.heartbeat)
        self._thread.start()

        self._cas_lock = threading.Lock()
        self._key_table = {}
        self._key_info = {}

    def become_leader(self):
        self.state = "LEADER"
        print('I am the leader')
        self._return_addresses = {}
        for i in self.otherNodes.keys():
            self._next_index.update({i: len(self.log)})
            self._match_index.update({i: 0})

    def handle(self, data):
        dict = pickle.loads(data)
        if type(dict) is not type({}):
            return
        self._lock.acquire()
        if dict['type'] == 'HB' or dict['type'] == 'HBR':
            self.handle_heartbeat(dict)
            self.check_log()
        elif dict['type'] == 'CR':
            self.handle_client_request(dict)
        elif dict['type'] == 'EL' or dict['type'] == 'ELR':
            self.handle_election(dict)
        self._lock.release()

    def check_log(self):
        if self.state == "LEADER":
            counts = {}
            for i in self._match_index.values():
                counts.update({i: counts.get(i, 0) + 1})
            for k, v in counts.items():
                if k > self.commitIndex and v > (len(self.otherNodes.values()) + 1) / 2 - 1:
                    self.commitIndex = k
        if self.commitIndex > self._last_applied:
            i = self._last_applied + 1
            while i <= self.commitIndex:
                self.apply(i)
                i += 1

    def send(self, address, data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(address)
                sock.sendall(pickle.dumps(data, pickle.HIGHEST_PROTOCOL))
            except Exception as err:
                print(f'unable to connect {address} {err}')

    def send_to_node(self, id, data):
        self.send(self.otherNodes[id], data)

    def apply(self, index):
        self._last_applied = index
        if self.log[index] == 'init':
            return

        st = 'fail'
        parsed = self.log[index].split()
        if parsed[0] == 'pop':
            if self._key_table.get(parsed[1], 0) == 1:
                st = 'locked'
            else:
                st = self.hashTable.pop(parsed[1], 'fail')
                print(f'Hash table changed: {self.hashTable}')
        elif parsed[0] == 'set':
            if self._key_table.get(parsed[1], 0) == 1:
                st = 'locked'
            else:
                self.hashTable.update({parsed[1]: parsed[2]})
                st = 'success'
                print(f'Hash table changed: {self.hashTable}')
        elif parsed[0] == 'lock':
            st = self.lock_operation(parsed[1], parsed[2])
            print(self._key_table)
            if int(parsed[2]) == self.id:
                print('I got the lock ' + parsed[1])
        elif parsed[0] == 'unlock':
            st = self.unlock_operation(parsed[1], parsed[2])
            print(self._key_table)
            if int(parsed[2]) == self.id:
                print('I lost the lock ' + parsed[1])
        if self.state == "LEADER":
            self.send(self._return_addresses.pop(index), st)

    def _start_election(self):
        self.state = "CANDIDATE"
        self.currentTerm += 1
        self._votes_cnt = 1
        print('leader lost, starting election')
        el_pack = {
            'type': 'EL',
            'id': self.id,
            'term': self.currentTerm,
            'last_log_index': (len(self.log) - 1),
            'last_log_term': self.logTerms[-1],
            'last_applied': self._last_applied,
            'vote': 0
        }
        self._election_deadline = time.time() + self.random.randint(4, 10)
        for i in self.otherNodes.keys():
            self.send_to_node(i, el_pack)

    def handle_election(self, data):
        print(data)
        if self.state == "CANDIDATE" and data['type'] == 'ELR':
            if self.currentTerm < data['term']:
                self.currentTerm = data['term']
                self.state = "FOLLOWER"
                return
            if self._last_applied < data['last_applied']:
                self.state = "FOLLOWER"
                return
            self._election_deadline = time.time() + self.random.randint(4, 10)
            self._votes_cnt += data['vote']
            if self._votes_cnt > (len(self.otherNodes.keys()) + 1) / 2:
                self.become_leader()
        elif self.state == "FOLLOWER" and data['type'] == 'EL':
            self.leaderAlive = True
            reply = {
                'type': 'ELR',
                'id': self.id,
                'term': self.currentTerm,
                'last_applied': self._last_applied,
                'vote': 0
            }
            if self._last_applied > data['last_applied']:
                reply.update({'vote': 0})

            if self.currentTerm > data['term']:
                reply.update({'vote': 0})

            elif len(self.log) - 1 > data['last_log_index']:
                reply.update({'vote': 0})

            else:
                if len(self.log) <= data['last_log_index']:
                    reply.update({'vote': 1})
                    self.votedFor = data['id']
                elif self.logTerms[data['last_log_index']] == data['last_log_term'] and self.votedFor is None:
                    reply.update({'vote': 1})
                    self.votedFor = data['id']
                else:
                    reply.update({'vote': 0})
            self.send_to_node(data['id'], reply)
            return

    def handle_heartbeat(self, data):
        self.leaderAlive = True
        self.votedFor = None
        if self.state == "LEADER" and data['type'] == 'HBR':
            if data['term'] > self.currentTerm:
                self.currentTerm = data['term']
                self.state = "FOLLOWER"
                return
            if data['status'] == 'success':
                self._next_index[data['id']] += data['added']
                self._match_index[data['id']] = data['last_log']
                return
            elif data['status'] == 'fail':
                self._next_index[data['id']] -= 1
                return

        if self.state == "CANDIDATE":
            if data['term'] > self.currentTerm:
                self.state = "FOLLOWER"

        if self.state == "FOLLOWER" and data['type'] == 'HB':
            self.leaderId = data['id']
            if data['term'] > self.currentTerm:
                self.currentTerm = data['term']

            if self.currentTerm > data['term']:
                self.send_to_node(data['id'], {
                    'type': 'HBR',
                    'id': self.id,
                    'status': 'fail',
                    'term': self.currentTerm
                })
                return
            cond = True
            if len(self.log) > data['prev_log_index']:
                cond = self.logTerms[data['prev_log_index']] != data['prev_log_term']

            if cond:
                self.send_to_node(data['id'], {
                    'type': 'HBR',
                    'id': self.id,
                    'status': 'fail',
                    'term': self.currentTerm
                })
                return
            tmp = 1
            for term in data['log_terms']:
                if len(self.logTerms) <= data['prev_log_index'] + tmp:
                    break
                if term != self.logTerms[data['prev_log_index'] + tmp]:
                    for i in range(data['prev_log_index'] + tmp, len(self.logTerms)):
                        self.log.pop()
                        self.logTerms.pop()
                    break
                tmp += 1

            self.log.extend(data['log'])
            self.logTerms.extend(data['log_terms'])
            appended_entries = len(data['log'])
            if data['commit_index'] > self.commitIndex:
                self.commitIndex = min(data['commit_index'], len(self.log) - 1)

            self.send_to_node(data['id'], {
                'type': 'HBR',
                'term': self.currentTerm,
                'id': self.id,
                'status': 'success',
                'added': appended_entries,
                'last_log': (len(self.log) - 1)
            })

    def create_heartbeat(self, nodeId):
        log = self.log[self._next_index[nodeId]:]
        log_terms = self.logTerms[self._next_index[nodeId]:]
        pack = {
            'type': 'HB',
            'id': self.id,
            'term': self.currentTerm,
            'log': log,
            'log_terms': log_terms,
            'prev_log_index': (self._next_index[nodeId] - 1),
            'prev_log_term': self.logTerms[self._next_index[nodeId] - 1],
            'commit_index': self.commitIndex

        }
        return pack

    def heartbeat(self):
        while True:
            if self.state == "FOLLOWER":
                time.sleep(self.random.randint(10, 15))
                self._lock.acquire()
                if not self.leaderAlive:
                    self._start_election()
                self.leaderAlive = False
                self._lock.release()
            elif self.state == "LEADER":
                for i in self.otherNodes.keys():
                    self._lock.acquire()
                    self.send_to_node(i, self.create_heartbeat(i))
                    self._lock.release()
                    time.sleep(1)
            elif self.state == "CANDIDATE":
                self._lock.acquire()
                if time.time() > self._election_deadline:
                    if self.state == "CANDIDATE":
                        self.state = "FOLLOWER"
                self._lock.release()

    def handle_client_request(self, data):
        print(data)
        parsed = data['request'].split()
        if parsed[0] == 'get':
            if self._key_table.get(parsed[1], 0) == 1:
                self.send(data['client'], 'locked')
            else:
                self.send(data['client'], self.hashTable.get(parsed[1], 'No such key'))
            return
        elif parsed[0] == 'sleep':
            time.sleep(10)
        if self.state == "LEADER":
            self.log.append(data['request'])
            self._return_addresses.update({len(self.log) - 1: data['client']})
            self.logTerms.append(self.currentTerm)
            print(self.log)
        elif self.state == "FOLLOWER":
            if self.leaderId is not None:
                self.send_to_node(self.leaderId, data)

    def delete_key(self, key):
        self._cas_lock.acquire()
        self.send_unlock(key, self.myAddress)
        self._cas_lock.release()
        print('TTL ended')

    def compareAndSwap(self, key, new_val, old_val, version, ttl=TTL):
        self._cas_lock.acquire()
        if self._key_table.get(key, None) == old_val:
            tp = self._key_info.get(key, [None, key_life_thread(ttl, lambda: self.delete_key(key))])
            if tp[0] != version and tp[0] is not None:
                res = False
            else:
                self._key_table.update({key: new_val})
                if self.state == "LEADER":
                    tp[1].start()
                tp[0] = version
                self._key_info.update({key: tp})
                res = True

        else:
            res = False
        self._cas_lock.release()
        return res

    def relock_operation(self, lock_name, ver):
        self.compareAndSwap(lock_name, 1, 1, ver)

    def lock_operation(self, lock_name, ver):
        while not (self.compareAndSwap(lock_name, 1, None, ver) or
                   self.compareAndSwap(lock_name, 1, 0, ver)):
            pass
        return True

    def unlock_operation(self, lock_name, ver=None):
        if res := self.compareAndSwap(lock_name, 0, 1, ver):
            self._key_info.pop(lock_name)
        return res

    def send_lock(self, lock_name, client):
        package = {'type': 'CR',
                'client': client,
                'request': f'lock {lock_name} {self._key_info.get(lock_name, [self.id, None])[0]}'}
        if self.state == "LEADER":
            self.send(self.myAddress, package)
            return
        self.send_to_node(self.leaderId, package)

    def send_unlock(self, lock_name, client):
        package = {'type': 'CR',
                'client': client,
                'request': f'unlock {lock_name} {self._key_info.get(lock_name, [self.id, None])[0]}'}
        if self.state == "LEADER":
            self.send(self.myAddress, package)
            return
        self.send_to_node(self.leaderId, package)


class key_life_thread:
    def __init__(self, ttl, target):
        self.lock = threading.Lock()
        self.is_running = False
        self.target_time = time.time() + ttl
        self.ttl = ttl
        self.target = target
        self.thread = None

    def run(self):
        self.is_running = True
        while True:
            self.lock.acquire()
            if time.time() > self.target_time:
                break
            self.lock.release()
        self.target()
        self.is_running = False

    def start(self):
        if self.is_running:
            self.reload()
        else:
            self.thread = threading.Thread(target=self.run)
            self.thread.start()

    def reload(self):
        self.lock.acquire()
        self.target_time = time.time() + self.ttl
        self.lock.release()


if __name__ == '__main__':
    nodeAddresses = {}
    nodeID = int(sys.argv[2])
    for i in sys.argv[2:]:
        nodeAddresses.update({int(i): (sys.argv[1], int(i))})

    node = Node(nodeID, nodeAddresses)


    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            data = self.request.recv(1024)
            node.handle(data)


    with socketserver.TCPServer((sys.argv[1], nodeID), Handler) as server:
        server.serve_forever()
