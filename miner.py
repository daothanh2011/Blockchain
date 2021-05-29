import pdb

from flask import Flask, request
import requests
import sys
import random
import time
import torch
import os
import binascii
import copy

import json
from hashlib import sha256

import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

class Block:
    def __init__(self, idx, transactions=None, block_generation_time=None, previous_hash=None, nonce=0,
                 block_hash=None):
        self._idx = idx
        self._transactions = transactions or []
        self._block_generation_time = block_generation_time
        self._previous_hash = previous_hash
        self._nonce = nonce
        self._block_hash = block_hash

    def compute_hash(self, hash_previous_block=False):
        if hash_previous_block:
            block_content = self.__dict__
        else:
            block_content = copy.deepcopy(self.__dict__)
            block_content['_block_hash'] = None
        block_content = json.dumps(block_content, sort_keys=True)
        return sha256(block_content.encode()).hexdigest()

    def set_hash(self):
        self._block_hash = self.compute_hash()

    def nonce_increment(self):
        self._nonce += 1

    # getters of the private attribute
    def get_block_hash(self):
        return self._block_hash

    def get_previous_hash(self):
        return self._previous_hash

    def get_block_idx(self):
        return self._idx

    def get_transactions(self):
        # get the updates from this block
        return self._transactions

    # def remove_block_hash_to_verify_pow(self):
    #     self._block_hash = None

    # setters
    def set_previous_hash(self, hash_to_set):
        self._previous_hash = hash_to_set

    def add_verified_transaction(self, transaction):
        # after verified in cross_verification()
        self._transactions.append(transaction)

    def set_block_generation_time(self):
        self._block_generation_time = time.time()


class Miner:
    def __init__(self, idx):
        self._idx = idx
        self._is_miner = True
        # miner
        self._blockchain = Blockchain()
        self._ip_and_port = None
        ''' attributes for miners '''
        self._received_transactions = []
        # used to broadcast block for workers to do global updates
        self._associated_workers = set()
        # used to check if block size is full
        self._current_epoch_worker_nodes = set()
        # used in miner_broadcast_updates and block propogation
        self._current_epoch_miner_nodes = set()
        self._received_updates_from_miners = []
        # used in cross_verification and in the future PoS
        self._rewards = 0
        self._has_added_propagated_block = False
        self._propagated_block_pow = None

    ''' getters '''

    def get_idx(self):
        return self._idx

    def get_blockchain(self):
        return self._blockchain

    def get_current_epoch(self):
        return self._blockchain.get_chain_length() + 1

    def get_ip_and_port(self):
        return self._ip_and_port

    def is_propagated_block_added(self):
        return self._has_added_propagated_block

    def is_miner(self):
        return self.is_miner

    def get_propagated_block_pow(self):
        return self._propagated_block_pow

    def set_blockchain(self, blockchain):
        self._blockchain = blockchain

    def set_ip_and_port(self, ip_and_port):
        self._ip_and_port = ip_and_port

    def propagated_block_has_been_added(self):
        self._has_added_propagated_block = True

    def set_propagated_block_pow(self, pow_proof):
        self._propagated_block_pow = pow_proof

    def get_rewards(self, rewards):
        self._rewards += rewards

    def associate_worker(self, worker_address):
        self._associated_workers.add(worker_address)

    def get_all_current_epoch_workers(self):
        potential_new_peers = set()
        for node in peers:
            response = requests.get(f'{node}/get_role')
            if response.status_code == 200:
                if response.text == 'Worker':
                    response2 = requests.get(f'{node}/get_worker_epoch')
                    if response2.status_code == 200:
                        if int(response2.text) == self.get_current_epoch():
                            self._current_epoch_worker_nodes.add(node)
                            response3 = requests.get(f'{node}/get_peers')
                            if response3.status_code == 200:
                                potential_new_peers.update(response3.json()['peers'])

            else:
                return response.status_code
        peers.update(potential_new_peers)
        try:
            peers.remove(self._ip_and_port)
        except:
            pass

    def get_all_current_epoch_miners(self):
        potential_new_peers = set()
        for node in peers:
            response = requests.get(f'{node}/get_role')
            if response.status_code == 200:
                if response.text == 'Miner':
                    response2 = requests.get(f'{node}/get_miner_comm_round')
                    if response2.status_code == 200:
                        if int(response2.text) == device.get_current_epoch():
                            self._current_epoch_miner_nodes.add(node)
                            # a chance to update peer list as well
                            response3 = requests.get(f'{node}/get_peers')
                            if response3.status_code == 200:
                                potential_new_peers.update(response3.json()['peers'])
            else:
                return response.status_code
        peers.update(potential_new_peers)
        try:
            peers.remove(self._ip_and_port)
        except:
            pass

    def clear_all_vars_for_new_epoch(self):
        # clear updates from workers and miners from the last epoch
        self._associated_workers.clear()
        self._current_epoch_worker_nodes.clear()
        self._current_epoch_miner_nodes.clear()
        self._received_transactions.clear()
        self._received_updates_from_miners.clear()
        self._has_added_propagated_block = False
        self._propagated_block_pow = None

    def add_received_updates_from_miners(self, one_miner_updates):
        self._received_updates_from_miners.append(one_miner_updates)

    def request_associated_workers_download(self, pow_proof):
        block_to_download = self._blockchain.get_last_block()
        data = {"miner_id": self._idx, "miner_ip": self._ip_and_port, "block_to_download": block_to_download.__dict__,
                "pow_proof": pow_proof}
        headers = {'Content-Type': "application/json"}
        if self._associated_workers:
            for worker in self._associated_workers:
                response = requests.post(f'{worker}/download_block_from_miner', data=json.dumps(data), headers=headers)
                if response.status_code == 200:
                    print(f'Requested Worker {worker} to download the block.')
        else:
            print("No associated workers this round. Begin Next communication round.")

    def cross_verification(self):

        candidate_block = Block(idx=self._blockchain.get_chain_length())
        candidate_block.set_block_generation_time()
        if self._received_transactions:
            print("\nVerifying received updates from associated workers...")
            for update in self._received_transactions:
                if len(update['feature_gradients']['feature_gradients_list']) == DATA_DIM:
                    candidate_block.add_verified_transaction(update)
                    print(f"Updates from worker {update['worker_ip']}({update['worker_id']}) are verified.")
                    print(
                        f"This miner now sends rewards to the above worker for the provision of data by the SAMPLE_SIZE {SAMPLE_SIZE}")
                    data = {"miner_id": self._idx, "miner_ip": self._ip_and_port, "rewards": SAMPLE_SIZE}
                    headers = {'Content-Type': "application/json"}

                    response = requests.post(f"{update['worker_ip']}/get_rewards_from_miner", data=json.dumps(data),
                                             headers=headers)
                    if response.status_code == 200:
                        print(f'Rewards sent!\n')



                else:
                    print("Error cross-verification SELF")
                self.get_rewards(DATA_DIM)
        else:
            print("\nNo associated workers or no updates received from the associated workers. Skipping self verification...")
        print("After/Skip self verification, total rewards ", self._rewards)

        if self._received_updates_from_miners:
            for update_from_other_miner in self._received_updates_from_miners:
                print("\nVerifying broadcasted updates from other miners...")
                # pdb.set_trace()
                for update in update_from_other_miner['received_updates']:
                    if len(update['feature_gradients']['feature_gradients_list']) == DATA_DIM:
                        candidate_block.add_verified_transaction(update)
                        print(f"Updates from miner {update_from_other_miner['from_miner_ip']}({update_from_other_miner['from_miner_id']}) for worker {update['worker_ip']}({update['worker_id']}) are verified.")
                    else:
                        print("Error cross-verification OTHER MINER")

                    self.get_rewards(DATA_DIM)
        else:
            print("\nNo broadcasted updates received from other miners. Skipping cross verification.")
        print("After/Skip cross verification, total rewards ", self._rewards)
        print("\nCross verifications done")
        return candidate_block

    def miner_send_signal(self):
        print("send miner a signal")
        headers = {'Content-Type': "application/json"}
        if self._associated_workers:
            for worker in self._associated_workers:
                print("associated worker".worker)
                response = request.post(f'{worker}/get_signal_from_miner', headers=headers)
                if response.status_code == 200:
                    print(f' Send signal to worker successfuly')
        else:
            print("No associated workers.")

    # TEST FOR CONVERGENCE
    def cross_verification_skipped(self):
        candidate_block = Block(idx=self._blockchain.get_chain_length())
        candidate_block.set_block_generation_time()
        if self._received_transactions:
            for update in self._received_transactions:
                candidate_block.add_verified_transaction(update)
        if self._received_updates_from_miners:
            for update_from_other_miner in self._received_updates_from_miners:
                for update in update_from_other_miner['received_updates']:
                    candidate_block.add_verified_transaction(update)
        return candidate_block

    def proof_of_work(self, candidate_block):

        if self._is_miner:
            current_hash = candidate_block.compute_hash()
            while not current_hash.startswith('0' * Blockchain.difficulty):
                candidate_block.nonce_increment()
                current_hash = candidate_block.compute_hash()
            candidate_block.set_hash()
            return current_hash, candidate_block
        else:
            print('Worker does not perform PoW.')

    def miner_receive_worker_updates(self, transaction):
        if self._is_miner:
            self._received_transactions.append(transaction)
            print(
                f"\nThis miner {self.get_ip_and_port()}({self._idx}) received updates from worker {transaction['this_worker_address']}({transaction['worker_id']})")

            if len(self._current_epoch_worker_nodes) == len(self._received_transactions):
                pass
        else:
            print("Worker cannot receive other workers' updates.")

    def miner_broadcast_updates(self):

        self.get_all_current_epoch_miners()

        data = {"miner_id": self._idx, "miner_ip": self._ip_and_port, "received_updates": self._received_transactions}
        headers = {'Content-Type': "application/json"}

        # broadcast the updates
        for miner in self._current_epoch_miner_nodes:
            response = requests.post(miner + "/receive_updates_from_miner", data=json.dumps(data), headers=headers)
            if response.status_code == 200:
                print(f'This miner {self._ip_and_port}({self._idx}) has sent unverified updates to miner {miner}')
                print('Press ENTER to continue...')
        return "ok"

    def miner_propagate_the_block(self, block_to_propagate, pow_proof):
        device.get_all_current_epoch_miners()

        data = {"miner_id": self._idx, "miner_ip": self._ip_and_port, "propagated_block": block_to_propagate.__dict__,
                "pow_proof": pow_proof}
        headers = {'Content-Type': "application/json"}

        for miner in self._current_epoch_miner_nodes:
            response = requests.post(miner + "/receive_propagated_block", data=json.dumps(data), headers=headers)
            if response.status_code == 200:
                print(
                    f'This miner {self.get_ip_and_port()}({self._idx}) has sent the propagated block to miner {miner}')
                print('Press ENTER to continue...')

    def miner_mine_block(self, block_to_mine):
        if self._is_miner:
            print("Mining the block...")
            if block_to_mine.get_transactions():
                # TODO
                # get the last block and add previous hash
                last_block = self._blockchain.get_last_block()
                if last_block is None:
                    # mine the genesis block
                    block_to_mine.set_previous_hash(None)
                else:
                    block_to_mine.set_previous_hash(last_block.compute_hash(hash_previous_block=True))
                pow_proof, mined_block = self.proof_of_work(block_to_mine)
                return pow_proof, mined_block
            else:
                print("No transaction to mine. \nGo to the next communication round.")
                return None, None

        else:
            print("Worker does not mine transactions.")

    def add_block(self, block_to_add, pow_proof):
        last_block = self._blockchain.get_last_block()
        if last_block is not None:
            last_block_hash = last_block.compute_hash(hash_previous_block=True)
            if block_to_add.get_previous_hash() != last_block_hash:
                return False

            if not self.check_pow_proof(block_to_add, pow_proof):
                return False

            block_to_add.set_hash()

            self._blockchain.append_block(block_to_add)
            return True
        else:
            # only check 2. above
            if not self.check_pow_proof(block_to_add, pow_proof):
                return False
            # add genesis block
            block_to_add.set_hash()
            self._blockchain.append_block(block_to_add)
            return True

    @staticmethod
    def check_pow_proof(block_to_check, pow_proof):
        return pow_proof.startswith('0' * Blockchain.difficulty) and pow_proof == block_to_check.compute_hash()

    def check_chain_validity(cls, chain_to_check):
        chain_len = chain_to_check.get_chain_length()
        if chain_len == 0:
            pass
        elif chain_len == 1:
            pass
        else:
            for block in chain_to_check[1:]:
                if cls.check_pow_proof(block, block.get_block_hash()) and block.get_previous_hash == chain_to_check[
                    chain_to_check.index(block) - 1].compute_hash(hash_previous_block=True):
                    pass
                else:
                    return False
        return True

    def consensus(self):
        longest_chain = None
        chain_len = self._blockchain.get_chain_length()

        for node in peers:
            response = requests.get(f'{node}/get_chain_meta')
            length = response.json()['length']
            chain = response.json()['chain']
            if length > chain_len and self.check_chain_validity(chain):
                # Longer valid chain found!
                chain_len = length
                longest_chain = chain

        if longest_chain:
            self._blockchain._chain = longest_chain
            return True

        return False


class Blockchain:
    # for PoW
    difficulty = 2

    def __init__(self):
        self._chain = []

    def get_chain_length(self):
        return len(self._chain)

    def get_last_block(self):
        if len(self._chain) > 0:
            return self._chain[-1]
        else:
            return None

    def append_block(self, block):
        self._chain.append(block)


''' App Starts Here '''

app = Flask(__name__)

DATA_DIM = 3
SAMPLE_SIZE = 5
SIGNAL = 1
# miner waits for 180s to fill its candidate block with updates from devices
MINER_WAITING_UPLOADS_PERIOD = 10

PROMPT = ">>>"

miner_accept_updates = True

peers = set()
device = Miner(binascii.b2a_hex(os.urandom(4)).decode('utf-8'))


def miner_set_wait_time():
    if device.is_miner():
        global miner_accept_updates
        miner_accept_updates = True
        print(f"{PROMPT} Miner wait time set to {MINER_WAITING_UPLOADS_PERIOD}s, waiting for updates...")
        time.sleep(MINER_WAITING_UPLOADS_PERIOD)
        miner_accept_updates = False
        print(f"{PROMPT} Miner done accepting updates in this communication round.")
    else:
        return "error"


@app.route('/get_role', methods=['GET'])
def return_role():
    return "Miner"

@app.route('/get_miner_comm_round', methods=['GET'])
def get_miner_comm_round():
    if device.is_miner():
        return str(device.get_current_epoch())
    else:
        return "error"

@app.route('/within_miner_wait_time', methods=['GET'])
def within_miner_wait_time():
    return "True" if miner_accept_updates else "False"

@app.route('/new_transaction', methods=['POST'])
def new_transaction():
    if miner_accept_updates:
        update_data = request.get_json()
        required_fields = ["worker_id", "worker_ip", "feature_gradients", "computation_time", "this_worker_address"]
        for field in required_fields:
            if not update_data.get(field):
                return "Invalid transaction(update) data", 404
        device.associate_worker(update_data['this_worker_address'])
        peers.add(update_data['this_worker_address'])
        update_data["tx_received_time"] = time.time()
        device.miner_receive_worker_updates(update_data)
    return "Success", 201


@app.route('/receive_updates_from_miner', methods=['POST'])
def receive_updates_from_miner():
    sender_miner_id = request.get_json()["miner_id"]
    sender_miner_ip = request.get_json()["miner_ip"]
    with_updates = request.get_json()["received_updates"]
    one_miner_updates = {"from_miner_ip": sender_miner_ip, "from_miner_id": sender_miner_id,
                         "received_updates": with_updates}
    print(f"\nReceived unverified updates from miner {sender_miner_ip}({sender_miner_id}).\n")
    if with_updates:
        device.add_received_updates_from_miners(one_miner_updates)
        print(f"The received broadcasted updates {with_updates}\n")
    else:
        print(f"The received broadcasted updates is empty, which is then tossed.\n")
    print("Press ENTER to continue...")
    return "Success", 200


@app.route('/receive_propagated_block', methods=['POST'])
def receive_propagated_block():
    if device.is_propagated_block_added():
        return "A fork has happened."
    sender_miner_id = request.get_json()["miner_id"]
    sender_miner_ip = request.get_json()["miner_ip"]
    pow_proof = request.get_json()["pow_proof"]
    propagated_block = request.get_json()["propagated_block"]
    # first verify this block id == chain length
    block_idx = propagated_block["_idx"]
    if int(block_idx) != device.get_blockchain().get_chain_length():
        return "The received propagated block is not sync with this miner's epoch."
    # # check pow proof
    # if pow_proof.startswith('0' * Blockchain.difficulty) and pow_proof == propagated_block.compute_hash(): DONE IN add_block
    # add this block to the chain
    reconstructed_block = Block(block_idx,
                                propagated_block["_transactions"],
                                propagated_block["_block_generation_time"],
                                propagated_block["_previous_hash"],
                                propagated_block["_nonce"],
                                propagated_block['_block_hash'])
    print(f"\nReceived a propagated block from {sender_miner_ip}({sender_miner_id})")
    print(reconstructed_block.__dict__, "\nWith PoW", pow_proof)
    if device.add_block(reconstructed_block, pow_proof):
        device.set_propagated_block_pow(pow_proof)
        device.propagated_block_has_been_added()
        note_text = "NOTE: A propagated block has been received, verified and added to the this miner's blockchain."
        print('=' * len(note_text))
        print(note_text)
        print('=' * len(note_text))
        print("Press ENTER to continue...")
        return "A propagated block has been mined and added to the blockchain."

@app.route('/')
def runApp():
    app.debug = True
    print(f"\n==================")
    print(f"|  Demo  |")
    print(f"==================\n")

    while True:
        time.sleep(20)
        print(f"Starting communication round {device.get_current_epoch()}...\n")
        print(f"{PROMPT} This is Miner with ID {device.get_idx()}\n")
        device.clear_all_vars_for_new_epoch()


        if not device.is_propagated_block_added():
            device.get_all_current_epoch_workers()
        else:
            print("NOTE: A propagated block has been added. Jump to request worker download.")
            pass

        if not device.is_propagated_block_added():
            time.sleep(15)
        else:
            print("NOTE: A propagated block has been added. Jump to request worker download.")
            pass
        time.sleep(5)
        if not device.is_propagated_block_added():

            device.miner_broadcast_updates()
        else:
            print("NOTE: A propagated block has been added. Jump to request worker download.")
            pass

        if not device.is_propagated_block_added():

            candidate_block = device.cross_verification()
        else:
            print("NOTE: A propagated block has been added. Jump to request worker download.")
            pass

        pow_proof, mined_block = None, None
        if not device.is_propagated_block_added():

            if not device.is_propagated_block_added():
                pow_proof, mined_block = device.miner_mine_block(candidate_block)
                if pow_proof == None or mined_block == None:
                    pass
            else:
                print("NOTE: A propagated block has been added. Jump to request worker download.")
        else:
            print("NOTE: A propagated block has been added. Jump to request worker download.")
            pass

        if not device.is_propagated_block_added():
            if pow_proof == None or mined_block == None:
                pass

            if not device.is_propagated_block_added():
                device.miner_propagate_the_block(mined_block, pow_proof)
            else:
                print("NOTE: A propagated block has been added. Jump to request worker download.")
        else:
            print("NOTE: A propagated block has been added. Jump to request worker download.")
            pass

        if not device.is_propagated_block_added():
            if pow_proof == None or mined_block == None:
                pass
            if device.add_block(mined_block, pow_proof):
                print("Its own block has been added.")
            else:
                print("NOTE: A propagated block has been added. Jump to request worker download.")
                pass

        if pow_proof == None or mined_block == None:
            pass
        if device.is_propagated_block_added():
            device.request_associated_workers_download(device.get_propagated_block_pow())
        else:
            device.request_associated_workers_download(pow_proof)

@app.route('/chain', methods=['GET'])
def display_chain():
    chain = json.loads(query_blockchain())["chain"]
    print("\nChain info requested and returned -")
    for block_iter in range(len(chain)):
        block_id_to_print = f"Block #{block_iter + 1}"
        print()
        print('=' * len(block_id_to_print))
        print(block_id_to_print)
        print('=' * len(block_id_to_print))
        block = chain[block_iter]
        for tx_iter in range(len(block["_transactions"])):
            print(f"\nTransaction {tx_iter + 1}\n", block["_transactions"][tx_iter], "\n")
        print("_block_generation_time", block["_block_generation_time"])
        print("_previous_hash", block["_previous_hash"])
        print("_nonce", block["_nonce"])
        print("_block_hash", block["_block_hash"])
    return "Chain Returned in Port Console"


@app.route('/get_chain_meta', methods=['GET'])
def query_blockchain():
    chain_data = []
    for block in device.get_blockchain()._chain:
        chain_data.append(block.__dict__)
    return json.dumps({"length": len(chain_data),
                       "chain": chain_data,
                       "peers": list(peers)})


@app.route('/get_peers', methods=['GET'])
def query_peers():
    return json.dumps({"peers": list(peers)})


# TODO helper function used in register_with_existing_node() only while registering node
def sync_chain_from_dump(chain_dump):
    for block_data in chain_dump:
        block = Block(block_data["_idx"],
                      block_data["_transactions"],
                      block_data["_block_generation_time"],
                      block_data["_previous_hash"],
                      block_data["_nonce"])
        pow_proof = block_data['_block_hash']
        added = device.add_block(block, pow_proof)
        if not added:
            raise Exception("The chain dump is tampered!!")
        else:
            pass

@app.route('/register_node', methods=['POST'])
def register_new_peers():
    registrant_node_address = request.get_json()["registrant_node_address"]
    if not registrant_node_address:
        return "Invalid data", 400

    transferred_this_node_address = request.get_json()["registrar_node_address"]
    if device.get_ip_and_port() == None:
        device.set_ip_and_port(transferred_this_node_address)
    if device.get_ip_and_port() != transferred_this_node_address:
        return "This should never happen"

    peers.add(registrant_node_address)

    return query_blockchain()

@app.route('/register_with', methods=['POST'])
def register_with_existing_node():

    device.set_ip_and_port(request.host_url[:-1])

    registrar_node_address = request.get_json()["registrar_node_address"]
    if not registrar_node_address:
        return "Invalid request - must specify a registrar_node_address!", 400
    data = {"registrant_node_address": request.host_url[:-1], "registrar_node_address": registrar_node_address}
    headers = {'Content-Type': "application/json"}

    response = requests.post(registrar_node_address + "/register_node", data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        global peers
        peers.add(registrar_node_address)
        chain_data_dump = response.json()['chain']
        sync_chain_from_dump(chain_data_dump)

        peers.update(response.json()['peers'])
        try:
            peers.remove(device.get_ip_and_port())
        except:
            pass
        return "Registration successful", 200
    else:
        return "weird"

@app.route('/debug_peers', methods=['GET'])
def debug_peers():
    return repr(peers)
